Developing with Toil and toil-lib
---------------------------------

toil-lib contains individual functions and job functions that are useful when
developing toil workflows both generally and for bioinformatics. This is distict
from toil-scripts, which contains full pipelines developed on Toil that frequently
depend on toil-lib.
 
General information on developing workflows in Toil can be found here_.

.. _here: https://github.com/BD2KGenomics/toil/blob/master/docs/developing.rst

toil-lib packages
-----------------

Below is a breakdown of the types of functions contained in each toil-lib module.

tools: job functions that run various coommonly used bioinformatics tools
files: helper functions for manipulating local files
programs: functions for running, managing, and shutting down Docker containers
urls: helper functions for uploading and downloading files from various url types
spark: service jobs for spinning up toil-based spark clusters
validator: functions for checking the validity of tool output
