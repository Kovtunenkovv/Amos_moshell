lt all
confbl+
l cd /
l cd /home/shared/user/Voltage
$tdatelog = `date +%Y%m%d`
$logname = testlog_$tdatelog.txt
$tdate = `date +%Y%m%d%H%M`

get 0 ^managedElementId$ > $NE

ma test FieldReplaceableUnit

$i = 0
for $t in test
$i = $i + 1
$q = ldn($t)
if $q ~ =RRU- || $q ~ =BB-
pget $q,EnergyMeter pmPowerConsumption > $pow
pget $q,EnergyMeter pmVoltage > $volt
get $q productData > $tPD
$name = $tPD[productName]
get RRU productData > $tPD
$res_table[$i] = $NE|$tdate|$name|$q|$pow|$volt
fi
done

l+s $logname
pv $res_table
l-
