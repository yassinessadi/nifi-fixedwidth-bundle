```
/nifi-fixedwidth-bundle/
    └──── nifi-fixedwidth-processors/
        ├── nifi-fixedwidth-nar/
        └── pom.xml

/.gitignore
README.md
.git/

SetUp & Runing
```
mvn clean install
```

Unit Test:
```
mvn test -rf :nifi-fixedwidth-processors
```

To build :
```
mvn install -rf :nifi-fixedwidth-processors
```