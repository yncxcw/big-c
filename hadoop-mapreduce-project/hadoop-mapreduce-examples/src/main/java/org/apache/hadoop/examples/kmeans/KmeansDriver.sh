#!/bin/bash
THRESHOLD=0.01
if [ $# -ne 5 ]
then
  echo "Usage: ./KmeansDriver.sh [numMaps][numReds][input directory][output directory][num of iterations]"
  exit 1
fi
cd $hadoop
numMaps=$1;
numReds=$2;
input=$3;
output=$4;
iterations=$5;
count=0;
base_model="/localhome/hadoop1/work/kmeans/mr_centroids";
centroids_file="/localhome/hadoop1/work/kmeans/centroids_file";
cp $centroids_file $base_model
rm -r $hadoop/Results/* 2>&1

for (( i = 0; i < $iterations; i++ ))
do
  echo "========================"
  echo " Iteration # `expr $i + 1` "
  echo "========================"
  count=`expr $count + 1` 2>&1
  out="tmpout$count" 2>&1
  hadoop dfs -rmr $out 2>&1
  hadoop dfs -rmr $output 2>&1
  hadoop jar hadoop-0.20.3-dev-examples.jar kmeans -m $numMaps -r $numReds $input $out 2>&1
  #######################
  # Get results from HDFS
  #######################
  if [ $count -lt $iterations ]
  then
    echo "Getting results from HDFS for iteration $count ..."
    mkdir -p $hadoop/Results/$out 2>&1
    hadoop dfs -get $out/* $hadoop/Results/$out/ 2>&1
    #######################
    # concatenate results into a single file
    #######################
    echo "Generating new centroids file ..."
    cd $hadoop/Results/$out 2>&1
    tmpcc=0;
    files=$(ls) 2>&1
    for num in $files
    do
      tmpcc=`expr $tmpcc + 1`
    done
    cc=1;
    files=$(ls) 2>&1
    for file in $files
    do
        if [ -f $file ]
        then 
          while read -r centroidnum similarity movie_id movies_total movies_reviews
          do
               if [ -z $movie_id ]
               then
                   continue
               else
                   echo "$centroidnum       $similarity $movie_id $movies_total $movies_reviews" >> model_file_tmp 
               fi
          done < $file
        fi
        echo "file $cc of $tmpcc scanned ..."
        cc=`expr $cc + 1` 
    done
    cd $hadoop
    model_file="model_file_$count"
    mv $hadoop/Results/$out/model_file_tmp $hadoop/Results/$model_file 2>&1
    #######################
    # test condition
    #######################
    k=0;
    echo "checking test condition ..."
#    IFS = space, tab, newline by default, so no need to set it explicitly
    while read -r word1 word2 rest
    do
        sim_arr_1[$k]="$word2";
        echo "sim_arr_1[$k] = ${sim_arr_1[$k]}";
        k=`expr $k + 1`;
    done < $hadoop/Results/$model_file

    j=0;
#    IFS = space, tab, newline by default, so no need to set it explicitly
    while read -r word1 word2 rest
    do
        sim_arr_2[$j]="$word2";
        echo "sim_arr_2[$j] = ${sim_arr_2[$j]}";
        j=`expr $j + 1` 2>&1
    done < $base_model

    if [ $j -lt $k ]
    then
        len=$k;
    else
        len=$j;
    fi

    l=0;
    sum=0;
    while [ $l -lt $len ]
    do
        diff[$l]=`echo ${sim_arr_1[$l]} - ${sim_arr_2[$l]} | bc` 2>&1
        if [ $(echo "${diff[$l]} < 0.0" |bc) -eq 1 ]
        then
            diff[$l]=`echo 0 - ${diff[$l]} | bc` 2>&1
        fi
        sum=`echo $sum + ${diff[$l]} | bc` 2>&1
        l=`expr $l + 1` 2>&1
    done
    avg_diff=`echo "scale=4;$sum / $l" | bc` 2>&1
    echo "sum = $sum, number of centroids = $l, avg_diff = $avg_diff";

    #######################
    # if test condition not met, replace existing model file
    # copy the file to the slave nodes
    # else exit
    #######################
    m=`expr $i + 1`
    
    if [ $(echo "$avg_diff < $THRESHOLD" |bc) -eq 1 ]
    then
        echo "Iteration # $m converged with threshold = $avg_diff < $THRESHOLD"
        break
    elif [ $m -eq $iterations ]
    then
        echo "Last iteration done with threshold not achieved"
        echo "exiting..."
        break
    else
        echo "Threshold not achieved, continuing to next Iteration # `expr $i + 2`"
        cp $hadoop/Results/$model_file $base_model
        while read -r slave
        do
            scp $base_model $slave:$base_model
        done < $HADOOP_HOME/conf/slaves
    fi
  else # $count ge $iterations
    echo "Last iteration done"
  fi
done
echo "exiting..."
#######################
echo "cleaning ..."
#######################
count=0;
#hadoop dfs -mkdir $output 2>&1
#hadoop dfs -put $hadoop/Results/$model_file $output/part-00000 2>&1
hadoop dfs -mv $out $output 2>&1
for (( i = 1; i < $iterations; i++ )) 
do
  out="tmpout$i" 2>&1
  rm -r $hadoop/Results/$out 2>&1
  hadoop dfs -rmr $out
done
exit 0
