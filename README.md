# FLite

`FLite` is as a record mapping library for `Sqlite` written in `F#`.

The library is based on `Microsoft.Data.Sqlite`

The goal is to provide a simple, no fuss way to easily save and retrieve records and blobs from `Sqlite` databases. 

## Not an ORM

`Flite` is not an ORM. It is made to handle `F#` records. 

What `FLite` won't do:

* `Include` type queries.
    * Instead you can map a record to a verbatim sql query (see `Examples`)

* It will probably not replace the need to write `sql`.

### Why not?

Three main reasons:

1. Another ORM is not needed (and there are better options).

2. Tradition ORM's (like `Entity Framework`) feel a bit odd with `F#`. 
   In fact, one of the great things with `F#` is it removes a lot of problems ORM's solve. 
   For example, object tracking is less useful because of immutability.
   Also this should reduce the need for layers of abstract and `POCO`'s to represent databases.
   `F#` in general makes abstracting the infrastructure and configuration code a lot easier. 

3. `Sql` is a powerful language and lot of things a better handled in `sql`, 
   rather then trying to replace that functionality in another language.
   This ties in with of one of the main goals of the library to work with `sql` 
   rather than replace it.
   
# What does it do

What `FLite` does look to achieve is to remove some of the boiler plate and hassle. 

`Sqlite` is very useful database platform for a lot of situations, this library wants to enhance that.

The aim it to offer the features of tradition ORM's that are useful in `F#` and replace boilerplate infrastructure
so you can focus on configuration and business code.

Features include:

* Easy record mapping
* Blob support
* Transaction support

## Blobs

One of the most useful features `sqlite` is it's blob support.

`FLite` used a special type, `BlobField` to represent these values.

No other special handling is needed, this type wrappers around a stream.

***Make sure the target stream is readable***

If the target stream has existed for a while, 
it is possible to have gone out of it's original scope and been disposed.

For example:
```
let toStream data =
    use ms = new MemoryStream(data)
    ms

let createStream data = 
    let ms = toStream data
    // ms is disposed and will not be readable.
    BlobField.FromStream(ms)
```

## Transactions

Transactions allow you to execute a generic function with signature `QueryHandler -> 'R`.

This means you can execute any code within a transaction and a generic result can be returned.

A new `QueryHandler` is created with the the current connection and passed to the transaction.

### Notes

* In the transaction function, do not use the original `QueryHandler`, it will throw and exception.

* Transactions are not thread safe, if the connection is shared across multiple threads.
   * ...and probably not anyway. Though multi-thread writing to a `sqlite` database is a bit odd.
   * Use an agent pattern instead.
   
* Transactions use broad error handling. Try not to have too much non database related code in them that can throw exceptions.

## Examples

[To come]