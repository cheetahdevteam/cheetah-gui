# Welcome to Cheetah

[**Cheetah**](https://www.desy.de/~barty/cheetah/Cheetah/Welcome.html) is a set of programs for processing serial diffraction data from free electron laser sources, which enable taking home only the data with meaningful content.  Developed for use in our own experiments this is a sanity saver in many serial imaging experiments.

**Cheetah GUI** consists of a table of runs used to coordinate processing and simplify the viewing and monitoring of output.  Looking at your data is the first essential step in analysis so we try to make it as simple as possible.  Similarly, processing multiple runs on the cluster becomes a matter of a few mouse clicks with current processing status updated in the table.

**Cheetah** output is in the form of portable, standardised HDF5 and plain text files.  Viewers called from the GUI open and display these files. Data is grouped by run ID corresponding to one set of measurements.  Careful selection of dataset labels means the table indicates what data goes together and to provide a convenient result database for the whole experiment.

This repository contains new **Cheetah GUI** package which reqiures installation of [**OM**](https://www.ondamonitor.com/) (OnDA Monitor) for data processing. Both **Cheetah GUI** package and **cheetah** processing backend in **OM** are still under development and yet to be released. To use **cheetah** processing backend please install the [development version](https://github.com/omdevteam/om/tree/develop) of **OM**.

## Installation

**Cheetah GUI** requires

* python > 3.7
* om [(development version)](https://github.com/omdevteam/om/tree/develop)
* click
* h5py
* numpy
* scipy
* jinja2
* PyQt5
* pyqtgraph
* typing_extensions 

```
$ git clone https://github.com/cheetahdevteam/cheetah-gui.git
$ pip install .
```

