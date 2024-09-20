Commamd: 
  get SysM=1,TimeM=1,DateAndTime= #текущее время
  al/alt
  ue print -admitted #абон. ЛТЕ
  hget radiolinks #абон 3g BB
  lhsh BXP_1 vs r | grep Supply #вольтаж
  lga -s 20210224  #выгрузка логов  с датой  
  cvget + название cv #сохранить backup
  invrx #оптика,ксв,серийники
  sdi
  get ethernet
  sfp -a  |grep "Temperature (C)" #температура DUW
  /fruacc/du/sfpdump  |grep "Temperature (C)" #температура BB
  pmxe RRU Voltage_V
  pmxe RRU PowerConsumption_W|Voltage_V
RET:
  acc RetSubUnit forceCalibration
  hget ret availabilityStatus|calibrationStatus|electricalAntennaTilt|operationalState|userLabel|maxTilt|minTilt
  hget AntennaNearUnit uniqueid

Поменять полярность на аварии:
  hget alarmport #проверить полярность
  bl alarmport=3 #блокировка аварии
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=3 normallyOpen false(true) #изменение
  deb alarmport=3 #разблокировка аварии
  cvms newAlarm #сохранение на ББ с названием
  altk

GPS:
  sts #статус синхронизации
  gpsstatus #информация по спутникам
  acc Synchronization=1,TimeSyncIO=1,GnssInfo=1 gnssReceiverRestart #рестарт GPS-приемника
  get GnssInfo=1 multipleGnssWanted		#проверка системы синхронизации GPS, GLONASS, BDS
  set gnssinfo multipleGnssWanted 2		#BDS
  set gnssinfo multipleGnssWanted 17		#GPS_GLONASS  
GPS_priority PTP/GPS:
    hget RadioEquipmentClockReference syncRefType|priority|administrativeState
    bl RadioEquipmentClockReference
    set RadioEquipmentClockReference=1 priority 5
    set RadioEquipmentClockReference=4 priority 6 
    deb RadioEquipmentClockReference 
    cvms GPS_priority

Прописать 4 аварии BB:
  bl AlarmPort
  confb+
  gs+

  cr Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=1 normallyOpen false
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=1 perceivedSeverity 2
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=1 alarmSlogan POWER
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=1 userLabel POWER

  cr Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=2 normallyOpen false
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=2 normallyOpen false
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=2 perceivedSeverity 2
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=2 alarmSlogan RECTIFIER
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=2 userLabel RECTIFIER

  cr Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=3 normallyOpen false
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=3 normallyOpen false
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=3 perceivedSeverity 2
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=3 alarmSlogan POWER
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=3 userLabel POWER

  cr Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=4 normallyOpen false
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=4 normallyOpen false
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=4 perceivedSeverity 2
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=4 alarmSlogan TEMP HIGH
  set Equipment=1,FieldReplaceableUnit=BB-1,AlarmPort=4 userLabel TEMP HIGH

  deb AlarmPort
  st AlarmPort
  confb-
  gs-
  cvms newAlarm
