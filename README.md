# koding-challange
A challange project for Koding

[![Code Climate](https://codeclimate.com/github/ezeql/koding-challange/badges/gpa.svg)](https://codeclimate.com/github/ezeql/koding-challange)

Tested using these docker images:

``` docker pull rabbitmq:3.5.6-management ```

```docker pull postgres:9.5```

```docker pull redis:3.0.5```

```docker pull mongo:3.0.7```

##Details

~~The [rabbitmq-management](https://www.rabbitmq.com/management.html) plugin provides an HTTP-based API for publishing messages among many other things.~~
An HTTP endpoint was introduced. 

[Flooder](https://github.com/ezeql/koding-challange/blob/master/cmd/flooder/main.go) can be used to test it.


Requires PostgreSQL >= 9.5 due [ON CONFLICT aka UPSERT](www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT) use.

###Metrics:

Pull them using [expvarmon](https://github.com/divan/expvarmon)

```
expvarmon -ports="33333,44444,55555" -vars="counts.totalProccesed,counts.workerErrors,hitsPerSecond" -i=5s
```
###ToDo

- [ ] Mongo tests
- [ ] Stress testing
- [ ] More tests to Redis worker
- [ ] try many goros pulling from the same consumer
