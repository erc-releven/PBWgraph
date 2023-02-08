# PBWgraph

This repository is focused on the extraction of data from the 
[Prosopography of the Byzantine World](https://pbw2016.kdl.kcl.ac.uk/) project 
(also [here on Github](https://github.com/kingsdigitallab/pbw-django)) for 
import into a graph database. No particular effort is made to organise this
repository into a proper Python project.

Contents of the repository are as follows:

- `pbw.py`: probably the main module of interest for others. This is an
   SQLAlchemy-based ORM library for accessing data in the PBW database.
- `config.py`: used for reading database connection information. Copy the
   file `config-template.py` to `config.py` and edit as necessary.
- `dateparse.py`: a module for parsing the wild and wonderful variety of
   date strings given in PBW into actual calendar dates.
- `query.py`: a script that uses the `pbw` module. Useful for examples of
   how the ORM is structured.
- `RELEVEN/`: scripts oriented specifically toward the RELEVEN project
- `scripts/`: miscellaneous scripts trying to use the PBW data
- `tests`: test scripts to make sure these things work
