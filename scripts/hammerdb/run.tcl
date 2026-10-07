# One timed TPROC-C run: hammerdbcli auto run.tcl
source [file join [file dirname [info script]] common.tcl]
diset tpcc ${P}_driver       timed
diset tpcc ${P}_rampup       [env HDB_RAMPUP 1]
diset tpcc ${P}_duration     [env HDB_DURATION 10]
diset tpcc ${P}_allwarehouse [env HDB_ALLWAREHOUSE true]
diset tpcc ${P}_keyandthink  false
diset tpcc ${P}_total_iterations 10000000000
loadscript
vuset logtotemp 0
vuset showoutput 1
vuset vu [env HDB_VU 4]
vucreate
vurun
vudestroy
exit
