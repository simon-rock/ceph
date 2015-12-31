// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "FSMap.h"

#include <sstream>
using std::stringstream;


void Filesystem::dump(Formatter *f) const
{
  f->open_object_section("mdsmap");
  mds_map.dump(f);
  f->close_section();
  f->dump_int("ns", ns);
}

void FSMap::dump(Formatter *f) const
{
  f->dump_int("epoch", epoch);

  f->open_object_section("compat");
  compat.dump(f);
  f->close_section();

  f->open_object_section("standbys");
  for (const auto &i : standby_daemons) {
    f->open_object_section("info");
    i.second.dump(f);
    f->close_section();
    f->dump_int("epoch", standby_epochs.at(i.first));
  }
  f->close_section();

  f->open_object_section("filesystems");
  for (const auto fs : filesystems) {
    fs.second->dump(f);
  }
  f->close_section();
}

void FSMap::generate_test_instances(list<FSMap*>& ls)
{
  FSMap *m = new FSMap();

  std::list<MDSMap*> mds_map_instances;
  MDSMap::generate_test_instances(mds_map_instances);

  int k = 20;
  for (auto i : mds_map_instances) {
    auto fs = std::make_shared<Filesystem>();
    fs->ns = k++;
    fs->mds_map = *i;
    delete i;
    m->filesystems[fs->ns] = fs;
  }
  mds_map_instances.clear();

  ls.push_back(m);
}

void FSMap::print(ostream& out) 
{
  // TODO add a non-json print?
  JSONFormatter f;
  dump(&f);
  f.flush(out);
}



// FIXME: revisit this (in the overall health reporting) to
// give a per-filesystem health.
void FSMap::print_summary(Formatter *f, ostream *out)
{
  map<mds_role_t,string> by_rank;
  map<string,int> by_state;

  if (f) {
    f->dump_unsigned("epoch", get_epoch());
    for (auto i : filesystems) {
      auto fs = i.second;
      f->dump_unsigned("id", fs->ns);
      f->dump_unsigned("up", fs->mds_map.up.size());
      f->dump_unsigned("in", fs->mds_map.in.size());
      f->dump_unsigned("max", fs->mds_map.max_mds);
    }
  } else {
    *out << "e" << get_epoch() << ":";
    if (filesystems.size() == 1) {
      auto fs = filesystems.begin()->second;
      *out << " " << fs->mds_map.up.size() << "/" << fs->mds_map.in.size() << "/"
           << fs->mds_map.max_mds << " up";
    } else {
      for (auto i : filesystems) {
        auto fs = i.second;
        *out << " " << fs->mds_map.fs_name << "-" << fs->mds_map.up.size() << "/"
             << fs->mds_map.in.size() << "/" << fs->mds_map.max_mds << " up";
      }
    }
  }

  if (f) {
    f->open_array_section("by_rank");
    const auto all_info = get_mds_info();
    for (const auto &p : all_info) {
      const auto &info = p.second;
      string s = ceph_mds_state_name(info.state);
      if (info.laggy()) {
        s += "(laggy or crashed)";
      }

      const mds_namespace_t ns = mds_roles.at(info.global_id);

      if (info.rank != MDS_RANK_NONE) {
        if (f) {
          f->open_object_section("mds");
          f->dump_unsigned("filesystem_id", ns);
          f->dump_unsigned("rank", info.rank);
          f->dump_string("name", info.name);
          f->dump_string("status", s);
          f->close_section();
        } else {
          by_rank[mds_role_t(ns, info.rank)] = info.name + "=" + s;
        }
      } else {
        by_state[s]++;
      }
    }
  }
  if (f) {
    f->close_section();
  } else {
    if (!by_rank.empty()) {
      if (filesystems.size() > 1) {
        // Disambiguate filesystems
        std::map<std::string, std::string> pretty;
        for (auto i : by_rank) {
          std::ostringstream o;
          o << "[" << i.first.ns << " " << i.first.rank << "]";
          pretty[o.str()] = i.second;
        }
        *out << " " << pretty;
      } else {
        *out << " " << by_rank;
      }
    }
  }

  for (map<string,int>::reverse_iterator p = by_state.rbegin(); p != by_state.rend(); ++p) {
    if (f) {
      f->dump_unsigned(p->first.c_str(), p->second);
    } else {
      *out << ", " << p->second << " " << p->first;
    }
  }

  size_t failed = 0;
  size_t damaged = 0;
  for (auto i : filesystems) {
    auto fs = i.second;
    failed += fs->mds_map.failed.size();
    damaged += fs->mds_map.damaged.size();
  }

  if (failed > 0) {
    if (f) {
      f->dump_unsigned("failed", failed);
    } else {
      *out << ", " << failed << " failed";
    }
  }

  if (damaged > 0) {
    if (f) {
      f->dump_unsigned("damaged", damaged);
    } else {
      *out << ", " << damaged << " damaged";
    }
  }
  //if (stopped.size())
  //out << ", " << stopped.size() << " stopped";
}

void FSMap::get_health(list<pair<health_status_t,string> >& summary,
			list<pair<health_status_t,string> > *detail) const
{
  for (auto i : filesystems) {
    auto fs = i.second;

    // TODO: move get_health up into here so that we can qualify
    // all the messages with what filesystem they're talking about
    fs->mds_map.get_health(summary, detail);
  }
}

void FSMap::encode(bufferlist& bl, uint64_t features) const
{
  ENCODE_START(10, 10, bl);
  ::encode(epoch, bl);
  ::encode(next_filesystem_id, bl);
  ::encode(legacy_client_namespace, bl);
  ::encode(compat, bl);
  std::vector<Filesystem> fs_list;
  for (auto i : filesystems) {
    fs_list.push_back(*(i.second));
  }
  ::encode(fs_list, bl);
  ::encode(mds_roles, bl);
  ::encode(standby_daemons, bl, features);
  ::encode(standby_epochs, bl);
  ENCODE_FINISH(bl);
}

void FSMap::decode(bufferlist::iterator& p)
{
  // Because the mon used to store an MDSMap where we now
  // store an FSMap, FSMap knows how to decode the legacy
  // MDSMap format (it never needs to encode it though).
  Filesystem legacy_fs;
  MDSMap &legacy_mds_map = legacy_fs.mds_map;
  bool enabled = false;
  
  DECODE_START_LEGACY_COMPAT_LEN_16(10, 4, 4, p);
  if (struct_v < 10) {
    // Decoding an MDSMap (upgrade)
    ::decode(epoch, p);
    ::decode(legacy_mds_map.flags, p);
    ::decode(legacy_mds_map.last_failure, p);
    ::decode(legacy_mds_map.root, p);
    ::decode(legacy_mds_map.session_timeout, p);
    ::decode(legacy_mds_map.session_autoclose, p);
    ::decode(legacy_mds_map.max_file_size, p);
    ::decode(legacy_mds_map.max_mds, p);
    ::decode(legacy_mds_map.mds_info, p);
    if (struct_v < 3) {
      __u32 n;
      ::decode(n, p);
      while (n--) {
        __u32 m;
        ::decode(m, p);
        legacy_mds_map.data_pools.insert(m);
      }
      __s32 s;
      ::decode(s, p);
      legacy_mds_map.cas_pool = s;
    } else {
      ::decode(legacy_mds_map.data_pools, p);
      ::decode(legacy_mds_map.cas_pool, p);
    }

    // kclient ignores everything from here
    __u16 ev = 1;
    if (struct_v >= 2)
      ::decode(ev, p);
    if (ev >= 3)
      ::decode(legacy_mds_map.compat, p);
    else
      legacy_mds_map.compat = get_mdsmap_compat_set_base();
    if (ev < 5) {
      __u32 n;
      ::decode(n, p);
      legacy_mds_map.metadata_pool = n;
    } else {
      ::decode(legacy_mds_map.metadata_pool, p);
    }
    ::decode(legacy_mds_map.tableserver, p);
    ::decode(legacy_mds_map.in, p);
    ::decode(legacy_mds_map.inc, p);
    ::decode(legacy_mds_map.up, p);
    ::decode(legacy_mds_map.failed, p);
    ::decode(legacy_mds_map.stopped, p);
    if (ev >= 4)
      ::decode(legacy_mds_map.last_failure_osd_epoch, p);
    if (ev >= 6) {
      ::decode(legacy_mds_map.ever_allowed_snaps, p);
      ::decode(legacy_mds_map.explicitly_allowed_snaps, p);
    } else {
      legacy_mds_map.ever_allowed_snaps = true;
      legacy_mds_map.explicitly_allowed_snaps = false;
    }
    if (ev >= 7)
      ::decode(legacy_mds_map.inline_data_enabled, p);

    if (ev >= 8) {
      assert(struct_v >= 5);
      ::decode(enabled, p);
      ::decode(legacy_mds_map.fs_name, p);
    } else {
      if (epoch > 1) {
        // If an MDS has ever been started, epoch will be greater than 1,
        // assume filesystem is enabled.
        enabled = true;
      } else {
        // Upgrading from a cluster that never used an MDS, switch off
        // filesystem until it's explicitly enabled.
        enabled = false;
      }
    }

    if (ev >= 9) {
      ::decode(legacy_mds_map.damaged, p);
    }
    // We're upgrading, populate fs_list from the legacy fields
    assert(filesystems.empty());
    auto migrate_fs = std::make_shared<Filesystem>(); 

    *migrate_fs = legacy_fs;
    migrate_fs->ns = MDS_NAMESPACE_ANONYMOUS;
    migrate_fs->mds_map.fs_name = "default";
    legacy_client_namespace = migrate_fs->ns;
    compat = migrate_fs->mds_map.compat;
  } else {
    ::decode(epoch, p);
    ::decode(next_filesystem_id, p);
    ::decode(legacy_client_namespace, p);
    ::decode(compat, p);
    std::vector<Filesystem> fs_list;
    ::decode(fs_list, p);
    filesystems.clear();
    for (std::vector<Filesystem>::const_iterator fs = fs_list.begin(); fs != fs_list.end(); ++fs) {
      filesystems[fs->ns] = std::make_shared<Filesystem>(*fs);
    }

    ::decode(mds_roles, p);
    ::decode(standby_daemons, p);
    ::decode(standby_epochs, p);
  }

  DECODE_FINISH(p);
}


void Filesystem::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(ns, bl);
  bufferlist mdsmap_bl;
  mds_map.encode(mdsmap_bl, CEPH_FEATURE_PGID64 | CEPH_FEATURE_MDSENC);
  ::encode(mdsmap_bl, bl);
  ENCODE_FINISH(bl);
}

void Filesystem::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(ns, p);
  bufferlist mdsmap_bl;
  ::decode(mdsmap_bl, p);
  bufferlist::iterator mdsmap_bl_iter = mdsmap_bl.begin();
  mds_map.decode(mdsmap_bl_iter);
  DECODE_FINISH(p);
}

int FSMap::parse_filesystem(
      std::string const &ns_str,
      std::shared_ptr<Filesystem> *result
      ) const
{
  std::string ns_err;
  mds_namespace_t ns = strict_strtol(ns_str.c_str(), 10, &ns_err);
  if (!ns_err.empty() || filesystems.count(ns) == 0) {
    for (auto fs : filesystems) {
      if (fs.second->mds_map.fs_name == ns_str) {
        *result = fs.second;
        return 0;
      }
    }
    return -ENOENT;
  } else {
    *result = get_filesystem(ns);
    return 0;
  }
}

