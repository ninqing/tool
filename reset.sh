#! /bin/bash
user='kimnin'
datafile=/Users/kimnin/ssss
tarfile=/Users/kimnin/mysqld.tar.gz
ips=("127.0.0.1" "127.0.0.1" "127.0.0.1")

for ip in ${ips[@]}
do
	ssh $user@$ip "ps -ef | grep mysqld | grep -v grep| awk '{print \$2}' | xargs kill -9"
	echo "rm -rf $datafile/*;tar -xf $tarfile -C $datafile/"
	#ssh $user@$ip "rm -rf $datafile/*;tar -xf $tarfile -C $datafile/"
done
