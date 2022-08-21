==================================================
Merkle Science ``dataengineering`` Python Library
==================================================

.. image:: https://github.com/merklescience/dataengineering/actions/workflows/python-release.yml/badge.svg
   :target: https://github.com/merklescience/dataengineering/actions/workflows/python-release.yml
   :alt: Quality and Release

This library contains all the utilities that we use in the Data Engineering
team at Merkle Science.

.. contents::

----------------------------------------------------
Things that Belong Here
----------------------------------------------------
This is the library where you *should* put in the following. The scope may
change in the future to include other things.

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

-------------------------------
Development
-------------------------------

This project uses ``pre-commit`` to enforce certain rules. Make sure you
`install pre-commit <https://pre-commit.com/#install>`_ to use it.
Additionally, `install poetry <https://python-poetry.org/docs/#installation>`_
to be able to contribute to it. Note that neither ``poetry`` nor ``pre-commit``
are requirements for users.

Practices
================

1. We use ``black`` for autoformatting code.
2. We don't support direct commits to the ``main`` branch.
3. We use ``sphinx`` for documentation, which will be hosted later.
4. We use `Conventional Commits
   <https://www.conventionalcommits.org/en/v1.0.0/>`_, as followed by the
   Angular Team, to commit all messages. Commits following this structure will
   also trigger automated releases following `Semantic Versioning (SemVer)
   <https://semver.org/>`_

Conventional Commit Prefixes and SemVer
=========================================

Suggested prefixes are: ``fix: ``, ``build: ``, ``chore: ``, ``ci: ``, ``docs: ``,
``style: ``, ``refactor: ``, ``perf: `` and ``test: ``. Note that ``fix: ``,
will bump the *PATCH* version, ``feat: `` will bump the *MINOR* version and
any commit that contains *BREAKING CHANGE* will bump the *MAJOR* version.
