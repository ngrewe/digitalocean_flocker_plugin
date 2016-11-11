A Flocker Dataset Backend for DigitalOcean Block Storage
========================================================

This is a proof of concept, and not production ready. Enable by letting
`python setup.py install` install into a location searchable by flocker
and put the following into your `agent.yml` file:

```yaml
"dataset":
  "backend": "digitalocean_flocker_plugin"
  "token": "your-do-api-token"
```

Limitations
-----------

* DO only allows you to mount 5 volumes to a single droplet
* It is not yet enforced that clusters only span a single region
* Not extensively tested

License
-------
Copyright 2016 Niels Grewe

Licensed under the Apache License, Version 2.0.