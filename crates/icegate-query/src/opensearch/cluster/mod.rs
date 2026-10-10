//! Cluster-level endpoints. None of them reads a table, so a client polling them costs no catalog
//! access.

pub(crate) mod info;
