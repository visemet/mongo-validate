mongo-validate
==============

Checks for any inconsistencies between indexes and their associated
collections.

Setup
-----

Create a workspace directory for the project, and clone the repo
inside of it. (Assumes `GOPATH` has been set appropriately.)

    mkdir -p $GOPATH/src/github.com/visemet
    cd $GOPATH/src/github.com/visemet
    git clone https://github.com/visemet/mongo-validate
    cd mongo-validate

Download and install the dependencies for this project:

  - mgo.v2: [source][mgo-source] | [documentation][mgo-docs]

```
go get gopkg.in/mgo.v2
```

### Build

    export GOBIN=bin
    go install validate/main/validate.go

License
-------

The library is available under the MIT License. For more information,
see the [license file][license] in the GitHub repository.

  [license]:    LICENSE
  [mgo-source]: https://github.com/go-mgo/mgo/tree/v2
  [mgo-docs]:   https://godoc.org/gopkg.in/mgo.v2
