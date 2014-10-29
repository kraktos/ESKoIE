#!/bin/bash


DIR="/home/adutta/git/ESKoIE"
ROCKIT="/home/adutta/rockit"

# CHANGE TO THE RELEVAT DIRECTORY
cd $DIR/

if [ ! -d 'src/main/resources/output/ds_'$1'' ]; then
	mkdir 'src/main/resources/output/ds_'$1''
fi

echo "\n\n ======= RUNNING FULL REASONING FOR " $1 " ========"


# running full pipeline
java -jar ../EXECUTABLES/ESKOIE_MAPPER.jar $1 CONFIG.cfg


#DYNAMICALLY CREATE THE MODEL FILE

cat $ROCKIT'/modelBasic.mln' 'src/main/resources/output/ds_'$1'/domRanEvidence.db'  > $ROCKIT'/model.mln'

#CHANGE TO ROCKIT DIRECTORY
cd $ROCKIT/

# RUN INFERENCE ENGINE

java -Xmx20G -jar rockit-0.3.228.jar -input model.mln -data $DIR'/src/main/resources/output/ds_'$1'/AllEvidence.db' -output $DIR'/src/main/resources/output/ds_'$1'/outAll.db'

# COPY FILES


cp $DIR'/src/main/resources/output/ds_'$1'/outAll.db' $DIR'/src/main/resources/output/ds_'$1'/out.db'

cp $DIR'/src/main/resources/output/ds_'$1'/domRanEvidence.db' $DIR'/src/main/resources/output/ds_'$1'/domRanEvidence_A1.db'

