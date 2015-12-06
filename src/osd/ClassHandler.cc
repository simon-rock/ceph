// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "msg/Message.h"
#include "osd/OSD.h"
#include "ClassHandler.h"
#include "common/errno.h"
#include <fstream>

#include <dlfcn.h>

#include <map>

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include "common/config.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout


#define CLS_PREFIX "libcls_"
#define CLS_SUFFIX ".so"

/*
 * Object classes can be resolved to classes defined in Lua. The naming
 * convention for Lua files is "cls_<class-name>.lua.
 */
#define CLS_LUA_PREFIX "cls_"
#define CLS_LUA_SUFFIX ".lua"
#define CLS_LUA_MAX_SIZE 1<<20 // catch egregious mistake

int ClassHandler::open_class(const string& cname, ClassData **pcls)
{
  Mutex::Locker lock(mutex);
  ClassData *cls = _get_class(cname);
  if (cls->status != ClassData::CLASS_OPEN) {
    int r = _load_class(cls);
    if (r)
      return r;
  }
  *pcls = cls;
  return 0;
}

int ClassHandler::open_all_classes()
{
  dout(10) << __func__ << dendl;
  DIR *dir = ::opendir(cct->_conf->osd_class_dir.c_str());
  if (!dir)
    return -errno;

  char buf[offsetof(struct dirent, d_name) + PATH_MAX + 1];
  struct dirent *pde;
  int r = 0;
  while ((r = ::readdir_r(dir, (dirent *)&buf, &pde)) == 0 && pde) {
    if (pde->d_name[0] == '.')
      continue;
    if (strlen(pde->d_name) > sizeof(CLS_PREFIX) - 1 + sizeof(CLS_SUFFIX) - 1 &&
	strncmp(pde->d_name, CLS_PREFIX, sizeof(CLS_PREFIX) - 1) == 0 &&
	strcmp(pde->d_name + strlen(pde->d_name) - (sizeof(CLS_SUFFIX) - 1), CLS_SUFFIX) == 0) {
      char cname[PATH_MAX + 1];
      strncpy(cname, pde->d_name + sizeof(CLS_PREFIX) - 1, sizeof(cname) -1);
      cname[strlen(cname) - (sizeof(CLS_SUFFIX) - 1)] = '\0';
      dout(10) << __func__ << " found " << cname << dendl;
      ClassData *cls;
      r = open_class(cname, &cls);
      if (r < 0)
	goto out;
    }
  }
 out:
  closedir(dir);
  return r;
}

void ClassHandler::shutdown()
{
  for (map<string, ClassData>::iterator p = classes.begin(); p != classes.end(); ++p) {
    if (p->second.handle)
      dlclose(p->second.handle);
  }
  classes.clear();
}

ClassHandler::ClassData *ClassHandler::_get_class(const string& cname)
{
  ClassData *cls;
  map<string, ClassData>::iterator iter = classes.find(cname);

  if (iter != classes.end()) {
    cls = &iter->second;
  } else {
    cls = &classes[cname];
    dout(10) << "_get_class adding new class name " << cname << " " << cls << dendl;
    cls->name = cname;
    cls->handler = this;
  }
  return cls;
}

int ClassHandler::_load_lua_class(ClassData *cls)
{
  char fname[PATH_MAX];
  snprintf(fname, sizeof(fname),
      "%s/" CLS_LUA_PREFIX "%s" CLS_LUA_SUFFIX,
      cct->_conf->osd_lua_class_dir.c_str(),
      cls->name.c_str());
  dout(10) << "_load_lua_class " << cls->name << " from " << fname << dendl;

  struct stat st;
  int r = ::stat(fname, &st);
  if (r < 0) {
    r = -errno;
    dout(0) << __func__ << " could not stat class " << fname
      << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  if (st.st_size > CLS_LUA_MAX_SIZE) {
    dout(0) << __func__ << " lua script too big " << fname
      << ": " << st.st_size << " bytes" << dendl;
    return -EIO;
  }

  cls->lua_script.reserve(st.st_size);

  std::ifstream ifs(fname);

  cls->lua_script.assign(std::istreambuf_iterator<char>(ifs),
      std::istreambuf_iterator<char>());

  /*
   * Mark 'lua' as a class dependency.
   */
  ClassData *cls_dep = _get_class("lua");
  cls->dependencies.insert(cls_dep);
  if (cls_dep->status != ClassData::CLASS_OPEN)
    cls->missing_dependencies.insert(cls_dep);

  cls->lua_handler = true;

  return 0;
}

int ClassHandler::_load_class(ClassData *cls)
{
  // already open
  if (cls->status == ClassData::CLASS_OPEN)
    return 0;

  if (cls->status == ClassData::CLASS_UNKNOWN ||
      cls->status == ClassData::CLASS_MISSING) {
    char fname[PATH_MAX];
    snprintf(fname, sizeof(fname), "%s/" CLS_PREFIX "%s" CLS_SUFFIX,
	     cct->_conf->osd_class_dir.c_str(),
	     cls->name.c_str());
    dout(10) << "_load_class " << cls->name << " from " << fname << dendl;

    cls->handle = dlopen(fname, RTLD_NOW);
    if (!cls->handle) {
      struct stat st;
      int r = ::stat(fname, &st);
      if (r < 0) {
        r = -errno;
        dout(0) << __func__ << " could not stat class " << fname
                << ": " << cpp_strerror(r) << dendl;
        if (r == -ENOENT)
          r = _load_lua_class(cls);
      } else {
	dout(0) << "_load_class could not open class " << fname
      	        << " (dlopen failed): " << dlerror() << dendl;
      	r = -EIO;
      }
      // dlopen error may be cleared if Lua class found
      if (r) {
        cls->status = ClassData::CLASS_MISSING;
        return r;
      }
    }

    // cls->handle may be NULL here when the class resolved to a Lua script,
    // and classes defined in Lua don't currently support specification of
    // class dependencies.
    if (cls->handle) {
      cls_deps_t *(*cls_deps)();
      cls_deps = (cls_deps_t *(*)())dlsym(cls->handle, "class_deps");
      if (cls_deps) {
        cls_deps_t *deps = cls_deps();
        while (deps) {
          if (!deps->name)
            break;
          ClassData *cls_dep = _get_class(deps->name);
          cls->dependencies.insert(cls_dep);
          if (cls_dep->status != ClassData::CLASS_OPEN)
            cls->missing_dependencies.insert(cls_dep);
          deps++;
        }
      }
    }
  }

  // resolve dependencies
  set<ClassData*>::iterator p = cls->missing_dependencies.begin();
  while (p != cls->missing_dependencies.end()) {
    ClassData *dc = *p;
    int r = _load_class(dc);
    if (r < 0) {
      cls->status = ClassData::CLASS_MISSING_DEPS;
      return r;
    }
    
    dout(10) << "_load_class " << cls->name << " satisfied dependency " << dc->name << dendl;
    cls->missing_dependencies.erase(p++);
  }

  /*
   * A Lua class contains a statically defined method that dispatches to
   * `cls_lua`. Here we lookup the class and stash a pointer to the target
   * method.
   */
  if (cls->lua_handler) {
    ClassHandler::ClassData *lua_cls = _get_class("lua");
    assert(lua_cls && lua_cls->name == "lua" &&
        lua_cls->status == ClassData::CLASS_OPEN);

    ClassHandler::ClassMethod *lua_method;
    lua_method = lua_cls->_get_method("eval_bufferlist");
    assert(lua_method);

    cls->lua_method_proxy.cxx_func = lua_method->cxx_func;
    cls->lua_method_proxy.name = "lua_method_proxy";
    cls->lua_method_proxy.flags = CLS_METHOD_RD | CLS_METHOD_WR;
    cls->lua_method_proxy.cls = cls;
  }
  
  // initialize (non-Lua classes)
  if (cls->handle) {
    void (*cls_init)() = (void (*)())dlsym(cls->handle, "__cls_init");
    if (cls_init) {
      cls->status = ClassData::CLASS_INITIALIZING;
      cls_init();
    }
  }
  
  dout(10) << "_load_class " << cls->name << " success" << dendl;
  cls->status = ClassData::CLASS_OPEN;
  return 0;
}



ClassHandler::ClassData *ClassHandler::register_class(const char *cname)
{
  assert(mutex.is_locked());

  ClassData *cls = _get_class(cname);
  dout(10) << "register_class " << cname << " status " << cls->status << dendl;

  if (cls->status != ClassData::CLASS_INITIALIZING) {
    dout(0) << "class " << cname << " isn't loaded; is the class registering under the wrong name?" << dendl;
    return NULL;
  }
  return cls;
}

void ClassHandler::unregister_class(ClassHandler::ClassData *cls)
{
  /* FIXME: do we really need this one? */
}

ClassHandler::ClassMethod *ClassHandler::ClassData::register_method(const char *mname,
                                                                    int flags,
								    cls_method_call_t func)
{
  /* no need for locking, called under the class_init mutex */
  if (!flags) {
    derr << "register_method " << name << "." << mname << " flags " << flags << " " << (void*)func
	 << " FAILED -- flags must be non-zero" << dendl;
    return NULL;
  }
  dout(10) << "register_method " << name << "." << mname << " flags " << flags << " " << (void*)func << dendl;
  ClassMethod& method = methods_map[mname];
  method.func = func;
  method.name = mname;
  method.flags = flags;
  method.cls = this;
  return &method;
}

ClassHandler::ClassMethod *ClassHandler::ClassData::register_cxx_method(const char *mname,
                                                                        int flags,
									cls_method_cxx_call_t func)
{
  /* no need for locking, called under the class_init mutex */
  dout(10) << "register_cxx_method " << name << "." << mname << " flags " << flags << " " << (void*)func << dendl;
  ClassMethod& method = methods_map[mname];
  method.cxx_func = func;
  method.name = mname;
  method.flags = flags;
  method.cls = this;
  return &method;
}

ClassHandler::ClassFilter *ClassHandler::ClassData::register_cxx_filter(
    const std::string &filter_name,
    cls_cxx_filter_factory_t fn)
{
  ClassFilter &filter = filters_map[filter_name];
  filter.fn = fn;
  filter.name = filter_name;
  filter.cls = this;
  return &filter;
}

ClassHandler::ClassMethod *ClassHandler::ClassData::_get_method(const char *mname)
{
  if (lua_handler)
    return &lua_method_proxy;
  map<string, ClassHandler::ClassMethod>::iterator iter = methods_map.find(mname);
  if (iter == methods_map.end())
    return NULL;
  return &(iter->second);
}

int ClassHandler::ClassData::get_method_flags(const char *mname)
{
  Mutex::Locker l(handler->mutex);
  ClassMethod *method = _get_method(mname);
  if (!method)
    return -ENOENT;
  return method->flags;
}

void ClassHandler::ClassData::unregister_method(ClassHandler::ClassMethod *method)
{
  /* no need for locking, called under the class_init mutex */
   map<string, ClassMethod>::iterator iter = methods_map.find(method->name);
   if (iter == methods_map.end())
     return;
   methods_map.erase(iter);
}

void ClassHandler::ClassMethod::unregister()
{
  cls->unregister_method(this);
}

void ClassHandler::ClassData::unregister_filter(ClassHandler::ClassFilter *filter)
{
  /* no need for locking, called under the class_init mutex */
   map<string, ClassFilter>::iterator iter = filters_map.find(filter->name);
   if (iter == filters_map.end())
     return;
   filters_map.erase(iter);
}

void ClassHandler::ClassFilter::unregister()
{
  cls->unregister_filter(this);
}

int ClassHandler::ClassMethod::exec(cls_method_context_t ctx,
    bufferlist& indata, bufferlist& outdata, const string& mname)
{
  /*
   * We intercept calls to a Lua handler, re-encode the parameters to include
   * the script being executed and the virtual method name, and point the
   * function being called at the `cls_lua` class.
   */
  if (cls->lua_handler) {
    bufferlist tmp_indata;
    ::encode(cls->lua_script, tmp_indata);
    ::encode(mname, tmp_indata);
    ::encode(indata, tmp_indata);
    indata = tmp_indata;
    assert(cxx_func); // we don't setup a C version
  }

  int ret;
  if (cxx_func) {
    // C++ call version
    ret = cxx_func(ctx, &indata, &outdata);
  } else {
    // C version
    char *out = NULL;
    int olen = 0;
    ret = func(ctx, indata.c_str(), indata.length(), &out, &olen);
    if (out) {
      // assume *out was allocated via cls_alloc (which calls malloc!)
      buffer::ptr bp = buffer::claim_malloc(olen, out);
      outdata.push_back(bp);
    }
  }
  return ret;
}

