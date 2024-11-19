#
#
#
#
"""
setup.py file for Cheetah GUI.
"""

from setuptools import find_packages, setup


version_fh = open("src/cheetah/__init__.py", "r")
version = version_fh.readlines()[-1].split("=")[1].strip().split('"')[1]
version_fh.close()
setup(
    name="cheetah",
    version=version,
    url="https://github.com/cheetahdevteam/cheetah-gui",
    license="GNU General Public License v3.0",
    author="Chetah Dev Team",
    author_email="alexandra.tolstikova@desy.de",
    description="Cheetah GUI",
    long_description=(
        """
        Cheetah is a set of programs for processing serial diffraction data from free 
        electron laser sources, which enable taking home only the data with meaningful 
        content.  Developed for use in our own experiments this is a sanity saver in 
        many serial imaging experiments.

        Cheetah GUI consists of a table of runs used to coordinate processing and 
        simplify the viewing and monitoring of output. Looking at your data is the first
        essential step in analysis so we try to make it as simple as possible. 
        Similarly, processing multiple runs on the cluster becomes a matter of a few
        mouse clicks with current processing status updated in the table.

        Cheetah output is in the form of portable, standardised HDF5 and plain text
        files. Viewers called from the GUI open and display these files. Data is grouped
        by run ID corresponding to one set of measurements. Careful selection of dataset
        labels means the table indicates what data goes together and to provide a 
        convenient result database for the whole experiment.
        """
    ),
    install_requires=[
        "click",
        "h5py",
        "mypy-extensions",
        "numpy",
        "jinja2",
        "scipy",
        "pyqt5",
        "pyqtgraph",
        "typing_extensions",
        "sortedcontainers",
        "ansi2html",
        "pyyaml",
        "ruamel.yaml",
        "ruamel.yaml.jinja2",
        "psutil",
    ],
    extras_require={
        "docs": ["mkdocs", "mkdocstring", "mkdocs-click", "mkdocs-material"],
    },
    entry_points={
        "console_scripts": [
            "cheetah_process_runs.py=cheetah.scripts.process_runs:main"
        ],
        "gui_scripts": [
            "cheetah_gui.py=cheetah.gui:main",
            "cheetah_viewer.py=cheetah.viewer:main",
            "cheetah_peakogram.py=cheetah.scripts.peakogram:main",
            "cheetah_hitrate.py=cheetah.scripts.hitrate:main",
            "online_cell_monitor.py=cheetah.scripts.cell_monitor:main",
        ],
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    platforms="any",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Natural Language :: English",
        "Intended Audience :: Science/Research",
    ],
)
