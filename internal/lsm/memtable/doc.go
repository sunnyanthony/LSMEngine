// Package memtable provides in-memory ordered indexes for recent writes.
//
// Tables own entry memory and support writer tracking so flush can wait for
// concurrent writers. Concrete implementations live under subpackages.
package memtable
