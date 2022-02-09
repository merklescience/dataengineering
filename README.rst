==================================================
Merkle Science ``dataengineering`` Python Library
==================================================

This library contains all the utilities that we use in the Data Engineering team at Merkle Science.

----------------------------------------------------
Things that Belong Here
----------------------------------------------------

* Utility Functions
* Custom Operators
* Sub-Dag Operators

-------------------------------------------------
Installation
-------------------------------------------------

On your local machine, or on a machine where you have SSH keys setup for github:

.. code-block:: bash

   pip install git+ssh://git@github.com/merklescience/dataengineering.git@$VERSION

On a deployment server, where you might not have setup the ssh keys:

.. code-block:: bash

   pip install git+https://$GITHUB_TOKEN@github.com/merklescience/dataengineering@$VERSION


It goes without saying that you'll need to set the ``GITHUB_TOKEN`` and the
``VERSION`` environment variables.
