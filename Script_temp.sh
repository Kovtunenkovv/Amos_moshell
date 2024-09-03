lt all
$password = RBSEricsson12
$tdatelog = `date +%Y%m%d`
$logname = log_$tdatelog.txt
get 0 productname > $nodeType
if $nodeType ~ RBS
get 0 logicalName > $NE
sfp -a | grep "| Vendor Name" | awk '{print ("$NE|V|"$5)}' >> /home/shared/name/user/$logname
sfp -a | grep "| Temperature (C)" | awk '{print ("$NE|T|"$5)}' >> /home/shared/user/temp/$logname
else
get 0 network > $NE
/fruacc/du/sfpdump  | grep "| Vendor Name" | awk '{print ("$NE|V|"$5)}' >> /home/shared/user/temp/$logname
/fruacc/du/sfpdump  | grep "| Temperature (C)" | awk '{print ("$NE|T|"$5)}' >> /home/shared/user/temp/$logname
fi
q
