# Polypheny Client

_Polypheny Client_ is a client for querying Polypheny-DB. It connects to Polypheny-DB via JDBC using the [JDBC Driver](https://github.com/polypheny/Polypheny-JDBC-Driver).

This client contains a [Chronos](https://github.com/chronos-eaas) connector. This allows to easily execute evaluation campaigns.


## Getting Started ##
Download the latest [polypheny-client.jar](https://github.com/polypheny/Polypheny-Client/releases/latest) from the release section. 
To start the query console, execute `polypheny-client.jar` by specifying the parameter `console` and the _hostname_ or _IP address_ of a running Polypheny-DB instance.

```
java -jar polypheny-client.jar console localhost
```


## Roadmap ##
See the [open issues](https://github.com/polypheny/Polypheny-Client/issues) for a list of proposed features (and known issues).


## Contributing ##
We highly welcome your contributions to Polypheny-DB. If you would like to contribute, please fork the repository and submit your changes as a pull request. Please consult our [Admin Repository](https://github.com/polypheny/Admin) for guidelines and additional information.

Please note that we have a [code of conduct](https://github.com/polypheny/Admin/blob/master/CODE_OF_CONDUCT.md). Please follow it in all your interactions with the project. 


## Credits ## 
The initial version of this client has been created by Silvan Heller and Manuel Hürbin.


## Acknowledgements
The Polypheny-DB project is supported by the Swiss National Science Foundation (SNSF) under the contract no. 200021_172763.


## License ##
The MIT License (MIT)