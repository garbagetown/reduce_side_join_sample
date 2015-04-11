# reduce_side_join_sample

```
# garbagetown at garbagetown.local in ~/dev/repos/reduce_side_join_sample on git:master x [0:39:19]
$ mv target/reduce_side_join_sample-1.0-SNAPSHOT.jar ~/dev/vagrants/cdh5/

# garbagetown at garbagetown.local in ~/dev/repos/reduce_side_join_sample on git:master x [0:40:03]
$ cp src/main/resources/part-* ~/dev/vagrants/cdh5

# garbagetown at garbagetown.local in ~/dev/repos/reduce_side_join_sample on git:master x [0:40:17]
$ cp src/main/resources/department.txt ~/dev/vagrants/cdh5

# garbagetown at garbagetown.local in ~/dev/repos/reduce_side_join_sample on git:master x [0:47:24]
$ ~/dev/vagrants/cdh5

# garbagetown at garbagetown.local in ~/dev/vagrants/cdh5 [0:47:44]
$ vagrant ssh
Last login: Sat Apr 11 14:59:39 2015 from 10.0.2.2
Welcome to your Vagrant-built virtual machine.

[vagrant@localhost ~]$ hadoop fs -mkdir input
[vagrant@localhost ~]$ hadoop fs -put /vagrant/part-* input
[vagrant@localhost ~]$ hadoop fs -put /vagrant/department.txt input

[vagrant@localhost ~]$ hadoop jar /vagrant/reduce_side_join_sample-1.0-SNAPSHOT.jar garbagetown.MyDriver input/part-e input/part-sh output
(snip)
[vagrant@localhost ~]$ hadoop fs -ls output
Found 3 items
-rw-r--r--   1 vagrant supergroup          0 2015-04-11 15:44 output/_SUCCESS
-rw-r--r--   1 vagrant supergroup         78 2015-04-11 15:44 output/part-r-00000
-rw-r--r--   1 vagrant supergroup         34 2015-04-11 15:44 output/part-r-00001

[vagrant@localhost ~]$ hadoop fs -cat output/part-r-00000
Georgi,Facello,M,d005,Development,88958
Parto,Bamford,M,d004,Production,43311

[vagrant@localhost ~]$ hadoop fs -cat output/part-r-00001
Bezalel,Simmel,F,d007,Sales,72527

[vagrant@localhost ~]$ yarn logs -applicationId application_1427781344221_0055
```

- [Hooked on Hadoop: Reduce-side joins in Java map-reduce](http://hadooped.blogspot.jp/2013/09/reduce-side-joins-in-java-map-reduce.html)
