# koding-challange
A challange project for Koding

[![Code Climate](https://codeclimate.com/github/ezeql/koding-challange/badges/gpa.svg)](https://codeclimate.com/github/ezeql/koding-challange)

Tested using these docker images:

``` docker pull rabbitmq:3.5.6-management ```

```docker pull postgres:9.5```

```docker pull redis:3.0.5```

```docker pull mongo:3.0.7```

Details

The [rabbitmq-management](https://www.rabbitmq.com/management.html) plugin provides an HTTP-based API for publishing messages among many other things.

Example
```
curl -H "Host: 127.0.0.1:15672" -H "Content-Type: application/json" -H "Authorization: Basic Z3Vlc3Q6Z3Vlc3Q=" --data-binary '{"properties":{},"routing_key":"","payload":"{ \"username\": \"kodingbot\", \"count\": 255,  \"metric\": \"kite_call\" }","payload_encoding":"string"}' --compressed http://127.0.0.1:15672/api/exchanges/%2f/logs/publish
```

Requires PostgreSQL >= 9.5 due [ON CONFLICT aka UPSERT](www.postgresql.org/docs/9.5/static/sql-insert.html#SQL-ON-CONFLICT) use.

Metrics:

https://github.com/divan/expvarmon

expvarmon -ports="33333,44444,55555" -vars="mem:memstats.Alloc,duration:Response.Mean,hitsPerSecond"
