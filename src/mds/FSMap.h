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


#ifndef CEPH_FSMAP_H
#define CEPH_FSMAP_H

#include <errno.h>

#include "include/types.h"
#include "common/Clock.h"
#include "msg/Message.h"
#include "mds/MDSMap.h"

#include <set>
#include <map>
#include <string>

#include "common/config.h"

#include "include/CompatSet.h"
#include "include/ceph_features.h"
#include "common/Formatter.h"
#include "mds/mdstypes.h"

class CephContext;

#define MDS_FEATURE_INCOMPAT_BASE CompatSet::Feature(1, "base v0.20")
#define MDS_FEATURE_INCOMPAT_CLIENTRANGES CompatSet::Feature(2, "client writeable ranges")
#define MDS_FEATURE_INCOMPAT_FILELAYOUT CompatSet::Feature(3, "default file layouts on dirs")
#define MDS_FEATURE_INCOMPAT_DIRINODE CompatSet::Feature(4, "dir inode in separate object")
#define MDS_FEATURE_INCOMPAT_ENCODING CompatSet::Feature(5, "mds uses versioned encoding")
#define MDS_FEATURE_INCOMPAT_OMAPDIRFRAG CompatSet::Feature(6, "dirfrag is stored in omap")
#define MDS_FEATURE_INCOMPAT_INLINE CompatSet::Feature(7, "mds uses inline data")
#define MDS_FEATURE_INCOMPAT_NOANCHOR CompatSet::Feature(8, "no anchor table")

#define MDS_FS_NAME_DEFAULT "cephfs"

/**
 * The MDSMap and any additional fields describing a particular
 * namespace.
 */
class Filesystem
{
  public:
  mds_namespace_t ns;
  MDSMap mds_map;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& p);

  Filesystem()
    :
      ns(MDS_NAMESPACE_NONE)
  {
  }

  void dump(Formatter *f) const;

  /**
   * Return true if a daemon is already assigned as
   * STANDBY_REPLAY for the gid `who`
   */
  bool has_standby_replay(mds_gid_t who) const
  {
    for (const auto &i : mds_map.mds_info) {
      const auto &info = i.second;
      if (info.state == MDSMap::STATE_STANDBY_REPLAY
          && info.rank == mds_map.mds_info.at(who).rank) {
        return true;
      }
    }

    return false;
  }
};
WRITE_CLASS_ENCODER(Filesystem)

class FSMap {
public:

  // indicate startup standby preferences for MDS
  // of course, if they have a specific rank to follow, they just set that!
  static const mds_rank_t MDS_NO_STANDBY_PREF; // doesn't have instructions to do anything
  static const mds_rank_t MDS_STANDBY_ANY;     // is instructed to be standby-replay, may
                                               // or may not have specific name to follow
  static const mds_rank_t MDS_STANDBY_NAME;    // standby for a named MDS
  static const mds_rank_t MDS_MATCHED_ACTIVE;  // has a matched standby, which if up
                                               // it should follow, but otherwise should
                                               // be assigned a rank

protected:
  epoch_t epoch;
  uint64_t next_filesystem_id;
  mds_namespace_t legacy_client_namespace;
  CompatSet compat;

  std::map<mds_namespace_t, std::shared_ptr<Filesystem> > filesystems;

  // Remember which Filesystem an MDS daemon's info is stored in
  // (or in standby_daemons for MDS_NAMESPACE_NONE)
  std::map<mds_gid_t, mds_namespace_t> mds_roles;

  // For MDS daemons not yet assigned to a Filesystem
  std::map<mds_gid_t, MDSMap::mds_info_t> standby_daemons;
  std::map<mds_gid_t, epoch_t> standby_epochs;

public:

  friend class MDSMonitor;

  FSMap() 
    : epoch(0),
      next_filesystem_id(MDS_NAMESPACE_ANONYMOUS + 1),
      legacy_client_namespace(MDS_NAMESPACE_NONE)
  { }

  FSMap(const FSMap &rhs)
    :
      epoch(rhs.epoch),
      next_filesystem_id(rhs.next_filesystem_id),
      legacy_client_namespace(rhs.legacy_client_namespace),
      compat(rhs.compat),
      mds_roles(rhs.mds_roles),
      standby_daemons(rhs.standby_daemons),
      standby_epochs(rhs.standby_epochs)
  {
    for (auto &i : rhs.filesystems) {
      auto fs = i.second;
      filesystems[fs->ns] = std::make_shared<Filesystem>(*fs);
    }
  }

  std::map<mds_gid_t, MDSMap::mds_info_t> get_mds_info() const
  {
    std::map<mds_gid_t, MDSMap::mds_info_t> result;
    for (const auto &i : standby_daemons) {
      result[i.first] = i.second;
    }

    for (const auto &i : filesystems) {
      auto fs_info = i.second->mds_map.get_mds_info();
      for (auto j : fs_info) {
        result[j.first] = j.second;
      }
    }

    return result;
  }

  /**
   * Resolve daemon name to GID
   */
  mds_gid_t find_mds_gid_by_name(const std::string& s) const
  {
    const auto info = get_mds_info();
    for (const auto &p : info) {
      if (p.second.name == s) {
	return p.first;
      }
    }
    return MDS_GID_NONE;
  }

  /**
   * Resolve daemon name to status
   */
  const MDSMap::mds_info_t* find_by_name(const std::string& name) const
  {
    std::map<mds_gid_t, MDSMap::mds_info_t> result;
    for (const auto &i : standby_daemons) {
      if (i.second.name == name) {
        return &(i.second);
      }
    }

    for (const auto &i : filesystems) {
      const auto &fs_info = i.second->mds_map.get_mds_info();
      for (const auto &j : fs_info) {
        if (j.second.name == name) {
          return &(j.second);
        }
      }
    }

    return NULL;
  }

  bool gid_exists(mds_gid_t gid) const
  {
    return mds_roles.count(gid) == 0;
  }

  bool gid_has_rank(mds_gid_t gid) const
  {
    return gid_exists(gid) && mds_roles.at(gid) != MDS_NAMESPACE_NONE;
  }

  /**
   * Insert a new MDS daemon, as a standby
   */
  void insert(const MDSMap::mds_info_t &new_info)
  {
    mds_roles[new_info.global_id] = MDS_NAMESPACE_NONE;
    standby_daemons[new_info.global_id] = new_info;
    standby_epochs[new_info.global_id] = epoch;
  }

  /**
   * A daemon reports that it is STATE_STOPPED: remove it,
   * and the rank it held.
   */
  void stop(mds_gid_t who)
  {
    assert(mds_roles.at(who) != MDS_NAMESPACE_NONE);
    auto fs = filesystems.at(mds_roles.at(who));
    const auto &info = fs->mds_map.mds_info.at(who);
    fs->mds_map.up.erase(info.rank);
    fs->mds_map.in.erase(info.rank);
    fs->mds_map.stopped.insert(info.rank);

    fs->mds_map.mds_info.erase(who);
    mds_roles.erase(who);

    fs->mds_map.epoch = epoch;
  }

  /**
   * The rank held by 'who', if any, is to be relinquished.
   */
  void erase(mds_gid_t who, epoch_t blacklist_epoch)
  {
    if (mds_roles.at(who) == MDS_NAMESPACE_NONE) {
      standby_daemons.erase(who);
      standby_epochs.erase(who);
    } else {
      auto fs = filesystems.at(mds_roles.at(who));
      const auto &info = fs->mds_map.mds_info.at(who);
      if (info.state == MDSMap::STATE_CREATING) {
        // If this gid didn't make it past CREATING, then forget
        // the rank ever existed so that next time it's handed out
        // to a gid it'll go back into CREATING.
        fs->mds_map.in.erase(info.rank);
      } else {
        // Put this rank into the failed list so that the next available
        // STANDBY will pick it up.
        fs->mds_map.failed.insert(info.rank);
      }
      fs->mds_map.up.erase(who);
      fs->mds_map.mds_info.erase(who);
      fs->mds_map.last_failure_osd_epoch = blacklist_epoch;
      fs->mds_map.epoch = epoch;
    }

    mds_roles.erase(who);
  }

  /**
   * The rank held by 'who' is damaged
   */
  void damaged(mds_gid_t who, epoch_t blacklist_epoch)
  {
    assert(mds_roles.at(who) != MDS_NAMESPACE_NONE);
    auto fs = filesystems.at(mds_roles.at(who));
    mds_rank_t rank = fs->mds_map.mds_info[who].rank;

    fs->mds_map.last_failure_osd_epoch = blacklist_epoch;
    fs->mds_map.up.erase(who);
    fs->mds_map.damaged.insert(rank);

    mds_roles.erase(who);

    fs->mds_map.epoch = epoch;
  }

  /**
   * The rank `rank` is to be removed from the damaged list.
   */
  bool undamaged(const mds_namespace_t ns, const mds_rank_t rank)
  {
    auto fs = filesystems.at(ns);

    if (fs->mds_map.damaged.count(rank)) {
      fs->mds_map.damaged.erase(rank);
      fs->mds_map.failed.insert(rank);
      fs->mds_map.epoch = epoch;
      return true;
    } else {
      return false;
    }
  }

  void modify_filesystem(
      const mds_namespace_t ns,
      std::function<void(std::shared_ptr<Filesystem> )> fn)
  {
    auto fs = filesystems.at(ns);
    fn(fs);
    fs->mds_map.epoch = epoch;
  }

  /**
   * Given that gid exists in a filesystem or as a standby, return
   * a reference to its info.
   */
  const MDSMap::mds_info_t& get_info_gid(mds_gid_t gid) const
  {
    auto ns = mds_roles.at(gid);
    if (ns == MDS_NAMESPACE_NONE) {
      return standby_daemons.at(gid);
    } else {
      return filesystems.at(ns)->mds_map.mds_info.at(gid);
    }
  }

  void assign_standby_replay(
      const mds_gid_t standby_gid,
      const mds_namespace_t leader_ns,
      const mds_rank_t leader_rank)
  {
    assert(mds_roles.at(standby_gid) == MDS_NAMESPACE_NONE);
    assert(gid_exists(standby_gid));
    assert(!gid_has_rank(standby_gid));
    assert(standby_daemons.count(standby_gid));

    // Insert to the filesystem
    auto fs = filesystems.at(leader_ns);
    fs->mds_map.mds_info[standby_gid] = standby_daemons.at(standby_gid);
    fs->mds_map.mds_info[standby_gid].standby_for_rank = leader_rank;
    fs->mds_map.mds_info[standby_gid].state = MDSMap::STATE_STANDBY_REPLAY;

    // Remove from the list of standbys
    standby_daemons.erase(standby_gid);
    standby_epochs.erase(standby_gid);

    // Indicate that Filesystem has been modified
    fs->mds_map.epoch = epoch;
  }

  /**
   * Assign an MDS cluster rank to a standby daemon
   */
  void promote(
      mds_gid_t standby_gid,
      std::shared_ptr<Filesystem> filesystem,
      mds_rank_t assigned_rank)
  {
    assert(mds_roles.at(standby_gid) == MDS_NAMESPACE_NONE);
    assert(gid_exists(standby_gid));
    assert(!gid_has_rank(standby_gid));
    assert(standby_daemons.count(standby_gid));

    MDSMap &mds_map = filesystem->mds_map;

    // Insert daemon state to Filesystem
    mds_map.mds_info[standby_gid] = standby_daemons.at(standby_gid);
    MDSMap::mds_info_t &info = mds_map.mds_info[standby_gid];

    if (mds_map.stopped.count(assigned_rank)) {
      // The cluster is being expanded with a stopped rank
      info.state = MDSMap::STATE_STARTING;
      mds_map.stopped.erase(assigned_rank);
    } else if (!mds_map.is_in(assigned_rank)) {
      // The cluster is being expanded with a new rank
      info.state = MDSMap::STATE_CREATING;
    } else {
      // An existing rank is being assigned to a replacement
      info.state = MDSMap::STATE_REPLAY;
      mds_map.failed.erase(assigned_rank);
    }
    info.rank = assigned_rank;
    info.inc = ++mds_map.inc[assigned_rank];

    // Update the rank state in Filesystem
    mds_map.in.insert(assigned_rank);
    mds_map.up[assigned_rank] = standby_gid;

    // Remove from the list of standbys
    standby_daemons.erase(standby_gid);
    standby_epochs.erase(standby_gid);

    // Indicate that Filesystem has been modified
    mds_map.epoch = epoch;
  }

  /**
   * Update the state & state_seq for an MDS daemon, as a result
   * of notification from that daemon.
   */
  void update_state(
      mds_gid_t who,
      MDSMap::DaemonState state,
      version_t state_seq)
  {
    modify_daemon(who, [state, state_seq](MDSMap::mds_info_t *info) {
        info->state = state;
        info->state_seq = state_seq;
    });
  }

  /**
   * Forcibly set the state for a daemon, as a result of
   * an administrative request.
   */
  void force_state(
      const mds_gid_t who,
      const MDSMap::DaemonState state)
  {
    modify_daemon(who, [state](MDSMap::mds_info_t *info) {
        info->state = state;
    });
  }

  /**
   * A daemon has told us it's compat, and it's too new
   * for the one we had previously.  Impose the new one
   * on all filesystems.
   */
  void update_compat(CompatSet c)
  {
    // We could do something more complicated here to enable
    // different filesystems to be served by different MDS versions,
    // but this is a lot simpler because it doesn't require us to
    // track the compat versions for standby daemons.
    compat = c;
    for (auto i : filesystems) {
      i.second->mds_map.compat = c;
      i.second->mds_map.epoch = epoch;
    }
  }

  /**
   * Apply a mutation to the mds_info_t structure for a particular
   * daemon (identified by GID), and make appropriate updates to epochs.
   */
  void modify_daemon(
      mds_gid_t who,
      std::function<void(MDSMap::mds_info_t *info)> fn)
  {
    if (mds_roles.at(who) == MDS_NAMESPACE_NONE) {
      fn(&standby_daemons.at(who));
      standby_epochs[who] = epoch;
    } else {
      auto fs = filesystems[mds_roles.at(who)];
      auto &info = fs->mds_map.mds_info.at(who);
      fn(&info);

      fs->mds_map.epoch = epoch;
    }
  }

  std::shared_ptr<Filesystem> get_legacy_filesystem()
  {
    if (legacy_client_namespace == MDS_NAMESPACE_NONE) {
      return nullptr;
    } else {
      return filesystems.at(legacy_client_namespace);
    }
  }

  /**
   * A daemon has informed us of its offload targets
   */
  void update_export_targets(mds_gid_t who, const std::set<mds_rank_t> targets)
  {
    auto ns = mds_roles.at(who);
    modify_filesystem(ns, [who, &targets](std::shared_ptr<Filesystem> fs) {
      fs->mds_map.mds_info.at(who).export_targets = targets;
    });
  }

  const std::map<mds_namespace_t, std::shared_ptr<Filesystem> > &get_filesystems() const
  {
    return filesystems;
  }
  bool any_filesystems() const {return !filesystems.empty(); }
  bool filesystem_exists(mds_namespace_t ns) const
    {return filesystems.count(ns) > 0;}

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  std::shared_ptr<Filesystem> get_filesystem(mds_namespace_t ns) const
  {
    return filesystems.at(ns);
  }

  int parse_filesystem(
      std::string const &ns_str,
      std::shared_ptr<Filesystem> *result
      ) const;

  /**
   * Return true if this pool is in use by any of the filesystems
   */
  bool pool_in_use(int64_t poolid) const {
    for (auto const &i : filesystems) {
      if (i.second->mds_map.is_data_pool(poolid)
          || i.second->mds_map.metadata_pool == poolid) {
        return true;
      }
    }
    return false;
  }

#if 0
  const MDSMap::mds_info_t& get_mds_info(mds_role_t role) {
    assert(filesystems.count(role.ns));
    const auto &fs = filesystems.at(role.ns); 
    assert(fs->up.count(role.rank) && mds_info.count(fs->up[role.rank]));
    return mds_info[fs->up[role.rank]];
  }
#endif



  // FIXME: standby_for_rank is bogus now because it doesn't say which filesystem
  // this fn needs reworking to respect filesystem priorities and not have
  // a lower priority filesystem stealing MDSs needed by a higher priority
  // filesystem
  //
  // Speaking of... as well as having some filesystems higher priority, we need
  // policies like "I require at least one standby MDS", so that even when a
  // standby is available, a lower prio filesystem won't use it if that would
  // mean putting a higher priority filesystem into a non-redundant state.
  // Could be option of "require one MDS is standby" or "require one MDS
  // is exclusive standby" so as to distinguish the case where two filesystems
  // means two standbys vs where two filesystems means one standby
  mds_gid_t find_standby_for(mds_rank_t mds, const std::string& name) const
  {
    mds_gid_t result = MDS_GID_NONE;

    for (const auto &i : standby_daemons) {
      const auto &gid = i.first;
      const auto &info = i.second;
      assert(info.state == MDSMap::STATE_STANDBY
             || info.state == MDSMap::STATE_STANDBY_REPLAY);
      assert(info.rank != MDS_RANK_NONE);

      if (info.laggy()) {
        continue;
      }

      if (info.standby_for_rank == mds || (name.length() && info.standby_for_name == name)) {
        // It's a named standby for *me*, use it.
	return gid;
      } else if (info.standby_for_rank < 0 && info.standby_for_name.length() == 0)
        // It's not a named standby for anyone, use it if we don't find
        // a named standby for me later.
	result = gid;
    }

    return result;
  }

  mds_gid_t find_unused_for(mds_rank_t mds, const std::string& name,
                            bool force_standby_active) const {
    for (const auto &i : standby_daemons) {
      const auto &gid = i.first;
      const auto &info = i.second;
      assert(info.state == MDSMap::STATE_STANDBY);

      if (info.laggy() || info.rank >= 0)
        continue;

      if ((info.standby_for_rank == MDS_NO_STANDBY_PREF ||
           info.standby_for_rank == MDS_MATCHED_ACTIVE ||
           (info.standby_for_rank == MDS_STANDBY_ANY && force_standby_active))) {
        return gid;
      }
    }
    return MDS_GID_NONE;
  }

  mds_gid_t find_replacement_for(mds_rank_t mds, const std::string& name,
                                 bool force_standby_active) const {
    const mds_gid_t standby = find_standby_for(mds, name);
    if (standby)
      return standby;
    else
      return find_unused_for(mds, name, force_standby_active);
  }

  void get_health(list<pair<health_status_t,std::string> >& summary,
		  list<pair<health_status_t,std::string> > *detail) const;

  // mds states


#if 0
  /**
   * Get MDS daemon status by GID
   */
  MDSMap::DaemonState get_state_gid(mds_gid_t gid) const {
    std::map<mds_gid_t,MDSMap::mds_info_t>::const_iterator i = mds_info.find(gid);
    if (i == mds_info.end()) {
      return MDSMap::STATE_NULL;
    }
    return i->second.state;
  }


  //MDSMap::mds_info_t& get_info(mds_rank_t m) { assert(up.count(m)); return mds_info[up[m]]; }


  bool is_laggy_gid(mds_gid_t gid) const {
    if (!mds_info.count(gid))
      return false;
    std::map<mds_gid_t,MDSMap::mds_info_t>::const_iterator p = mds_info.find(gid);
    return p->second.laggy();
  }

  bool is_degraded(mds_namespace_t ns) const {   // degraded = some recovery in process.  fixes active membership and recovery_set.
    if (filesystems.count(ns) == 0) {
      // This namespace doesn't exist, so it isn't degraded
      return false;
    }

    if (!filesystems.at(ns)->failed.empty() || !filesystems.at(ns)->damaged.empty()) {
      return true;
    }
    for (auto &p : mds_info) {
      if (p.second.role.ns == ns && p.second.state >= MDSMap::STATE_REPLAY && p.second.state <= MDSMap::STATE_CLIENTREPLAY) {
        return true;
      }
    }

    return false;
  }
#endif

  std::shared_ptr<Filesystem> get_filesystem(const std::string &name)
  {
    for (auto &i : filesystems) {
      if (i.second->mds_map.fs_name == name) {
        return i.second;
      }
    }

    return nullptr;
  }

  /**
   * Get whether a rank is 'up', i.e. has
   * an MDS daemon's entity_inst_t associated
   * with it.
   */
#if 0
  bool have_inst(mds_role_t r) const {
    const auto fs = filesystems.at(r.ns);
    return fs->up.count(r.rank);
  }

  /**
   * Get the MDS daemon entity_inst_t for a rank
   * known to be up.
   */
  const entity_inst_t get_inst(mds_role_t r) const {
    const auto fs = filesystems.at(r.ns);
    return mds_info.at(fs->up.at(r.rank)).get_inst();
  }
  const entity_addr_t get_addr(mds_role_t r) const {
    const auto fs = filesystems.at(r.ns);
    assert(fs->up.count(r.rank));
    return mds_info.at(fs->up.at(r.rank)).addr;
  }
#endif

  /**
   * Get MDS rank state if the rank is up, else MDSMap::STATE_NULL
   */
#if 0
  MDSMap::DaemonState get_state(mds_role_t r) const {
    const auto fs = get_filesystem(r.ns);
    const auto u = fs->up.find(r.rank);
    if (u == fs->up.end()) {
      return MDSMap::STATE_NULL;
    }
    return get_state_gid(u->second);
  }

  void get_recovery_mds_set(mds_namespace_t ns, std::set<mds_rank_t>& s) const {
    std::shared_ptr<const Filesystem> fs = filesystems.at(ns);
    s = fs->failed;
    for (const auto i : fs->up) {
      auto &gid = i.second;
      auto &info = mds_info.at(gid);
      if (info.role.ns == ns && info.state >= MDSMap::STATE_REPLAY
                             && info.state <= MDSMap::STATE_STOPPING) {
        s.insert(info.rank);
      }
    }
  }

  void get_clientreplay_or_active_or_stopping_mds_set(mds_namespace_t ns,
      std::set<mds_rank_t>& s) const {
    for (auto p : mds_info) {
      auto info = p.second;
      if (info.role.ns == ns && info.state >= MDSMap::STATE_CLIENTREPLAY
                             && info.state <= MDSMap::STATE_STOPPING) {
        s.insert(info.rank);
      }
    }
  }
  void get_mds_set(mds_namespace_t ns,
      std::set<mds_rank_t>& s, MDSMap::DaemonState state) const {
    for (auto p : mds_info) {
      auto info = p.second;
      if (info.role.ns == ns && info.state == state) {
        s.insert(info.rank);
      }
    }
  } 
  void get_active_mds_set(mds_namespace_t ns, std::set<mds_rank_t>& s) const {
    get_mds_set(ns, s, MDSMap::STATE_ACTIVE);
  }

  /**
   * How many of the MDSs assigned to this namespace are in this state?
   */
  unsigned get_num_mds(std::shared_ptr<const Filesystem> fs, int state) const {
    unsigned n = 0;
    for (const auto & p : fs->up) {
      const auto &gid = p.second;
      const auto &info = mds_info.at(gid);
      if (info.state == state) {
        ++n;
      }
    }
    return n;
  }

  bool is_resolving(mds_namespace_t ns) const {
    const auto &fs = filesystems.at(ns);
    return
      get_num_mds(fs, MDSMap::STATE_RESOLVE) > 0 &&
      get_num_mds(fs, MDSMap::STATE_REPLAY) == 0 &&
      fs->failed.empty();
  }

  bool is_rejoining(mds_namespace_t ns) const {  
    const auto &fs = filesystems.at(ns);
    // nodes are rejoining cache state
    return 
      get_num_mds(fs, MDSMap::STATE_REJOIN) > 0 &&
      get_num_mds(fs, MDSMap::STATE_REPLAY) == 0 &&
      get_num_mds(fs, MDSMap::STATE_RECONNECT) == 0 &&
      get_num_mds(fs, MDSMap::STATE_RESOLVE) == 0 &&
      fs->failed.empty();
  }


  bool is_boot(mds_role_t r) const { return get_state(r) == MDSMap::STATE_BOOT; }
  bool is_creating(mds_role_t r) const { return get_state(r) == MDSMap::STATE_CREATING; }
  bool is_starting(mds_role_t r) const { return get_state(r) == MDSMap::STATE_STARTING; }
  bool is_replay(mds_role_t r) const   { return get_state(r) == MDSMap::STATE_REPLAY; }
  bool is_resolve(mds_role_t r) const  { return get_state(r) == MDSMap::STATE_RESOLVE; }
  bool is_reconnect(mds_role_t r) const { return get_state(r) == MDSMap::STATE_RECONNECT; }
  bool is_rejoin(mds_role_t r) const   { return get_state(r) == MDSMap::STATE_REJOIN; }
  bool is_clientreplay(mds_role_t r) const { return get_state(r) == MDSMap::STATE_CLIENTREPLAY; }
  bool is_active(mds_role_t r) const  { return get_state(r) == MDSMap::STATE_ACTIVE; }
  bool is_stopping(mds_role_t r) const { return get_state(r) == MDSMap::STATE_STOPPING; }
  bool is_active_or_stopping(mds_role_t r) const {
    return is_active(r) || is_stopping(r);
  }
  bool is_clientreplay_or_active_or_stopping(mds_role_t r) const {
    return is_clientreplay(r) || is_active(r) || is_stopping(r);
  }

  bool is_followable(mds_role_t r) const {
    return (is_resolve(r) ||
	    is_replay(r) ||
	    is_rejoin(r) ||
	    is_clientreplay(r) ||
	    is_active(r) ||
	    is_stopping(r));
  }

  mds_role_t get_role_gid(mds_gid_t gid) {
    if (mds_info.count(gid))
      return mds_info[gid].role;
    return mds_role_t();
  }

  int get_inc_gid(mds_gid_t gid) {
    if (mds_info.count(gid))
      return mds_info[gid].inc;
    return -1;
  }
#endif
  void encode(bufferlist& bl, uint64_t features) const;
  void decode(bufferlist::iterator& p);
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    decode(p);
  }


  void print(ostream& out);
  void print_summary(Formatter *f, ostream *out);

  void dump(Formatter *f) const;
  static void generate_test_instances(list<FSMap*>& ls);
};
WRITE_CLASS_ENCODER_FEATURES(FSMap)

inline ostream& operator<<(ostream& out, FSMap& m) {
  m.print_summary(NULL, &out);
  return out;
}

#endif
