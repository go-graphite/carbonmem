Changes
=================================================

[Fix] - bugfix

**[Breaking]** - breaking change

[Feature] - new feature

[Improvement] - non-breaking improvement

[Code] - code quality related change that shouldn't make any significant difference for end-user

CHANGELOG
---------
**0.7**
 - [Fix] Make it compatible with both pb2 and pb3. Incompatibility was introduced in 0.6
 - [Fix] Make it compatible with recent carbonzipper definitions
 - [Code] commit vendored dependencies

**0.6.1**
 - [Fix] Fix panic on bad queries
 - [Improvement] Introduce vendoring and Makefile

**0.6**
 - **[Breaking]** Migrate to protobuf3

Notes on upgrading to 0.6.0:

You need to upgrade carbonzipper or go-carbon to the version that supports protobuf3

**<=0.5**
There is no dedicated changelog for older versions of carbonmem. Please see commit log for more information about what changed for each commit.
