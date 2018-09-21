#!/bin/bash

ISO2S='AL AD AM AT BY BE BA BG CH CY CZ DE DK EE ES FO FI FR GB GE GI GR HU HR IE IS IT LT LU LV MC MK MT NO NL PL PT RO RU SE SI SK SM TR UA VA'

for iso2 in $ISO2S;
do
    grep -l ":)" $iso2.out &> /dev/null
    if [[ $? -ne 0 ]];
    then
	echo $iso2
    fi
done


