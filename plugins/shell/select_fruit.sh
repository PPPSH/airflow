FRUIT=$1
if [ $FRUIT == APPLE ]; then 
	echo "You selected Apple!"
elif [ $FRUIT == ORAGNE ]; then
	echo "You selected Oragne!"
elif [ $FRUIT == GRAPE ]; then
	echo "You selected Grape!"
else
	echo "You selected other Fruit!"
fi
