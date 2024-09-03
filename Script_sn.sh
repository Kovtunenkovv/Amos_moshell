lt all
confbl+
l cd /
l cd /home/shared/user/sn
$tdatelog = `date +%Y%m%d`
$logname = testlog_$tdatelog.txt

get 0 siteLocation > $Site
get 0 network > $NE
$tdate = `date +%Y%m%d`

ma MO_Group FieldReplaceableUnit fieldReplaceableUnitId BB*||fieldReplaceableUnitId RRU*
$i = 0
for $MO in MO_Group
$i = $i + 1
$MOrdn = rdn($MO)
get $MO productData > $tPD
$t1 = $tPD[productionDate]
$t2 = $tPD[productName]
$t3 = $tPD[productNumber]
$t4 = $tPD[productRevision]
$t5 = $tPD[serialNumber]
$res_table[$i] = $Site|$NE|$tdate|$MOrdn|$t1|$t2|$t3|$t4|$t5
done
l+s $logname
pv $res_table
l-
