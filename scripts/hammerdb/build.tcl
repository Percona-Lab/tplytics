# Build the TPROC-C schema: hammerdbcli auto build.tcl
source [file join [file dirname [info script]] common.tcl]
diset tpcc ${P}_count_ware [env HDB_WAREHOUSES 10]
diset tpcc ${P}_num_vu     [env HDB_VU 4]
if { $P eq "mysql" } { diset tpcc mysql_storage_engine innodb }
buildschema
vudestroy
puts "TPLYTICS: build finished"
exit
