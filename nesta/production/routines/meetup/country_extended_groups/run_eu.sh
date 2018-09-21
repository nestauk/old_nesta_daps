#!/bin/bash

ISO2S='AL AD AM AT BY BE BA BG CH CY CZ DE DK EE ES FO FI FR GB GE GI GR HU HR IE IS IT LT LU LV MC MK MT NO NL PL PT RO RU SE SI SK SM TR UA VA'

# Include the countries more than once to account for failures
#ISO2S='GB DE FR IT ES AT AL AD AM AT BY BE BA BG CH CY CZ DE DK EE ES FO FI FR GB GE GI GR HU HR IE IS IT LT LU LV MC MK MT NO NL PL PT RO RU SE SI SK SM TR UA VA AL AD AM AT BY BE BA BG CH CY CZ DE DK EE ES FO FI FR GB GE GI GR HU HR IE IS IT LT LU LV MC MK MT NO NL PL PT RO RU SE SI SK SM TR UA VA'

DATE=2018-09-13

for iso2 in $ISO2S;
do
    echo $iso2
    ~/miniconda3/envs/py36/bin/luigi --module country_extended_groups RootTask --iso2 $iso2 --category 34 --production --date $DATE &> ${iso2}.out
done

echo "done"
