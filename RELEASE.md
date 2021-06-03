# Release Process

The Agave Science API release process is as follows:

1. An issue is created proposing a new release with a changelog since the last release
2. At least one of the [approvers](OWNERS_ALIASES) must approve this release
8. The Maven `${project.version}` value is updated in all Maven `pom.xml` files.
6. An approver creates a release tag in the form `vX.Y`
7. New Docker images are built, tagged, and pushed to the public Docker Hub with the `${project.version} for each image.
8. The public tenant is updated with the latest version.   
9. The release issue is closed
10. An announcement message is posted to [status.agaveplatform.org](https://status.agaveplatform.org/)` and distributed to subscribers.
11. The topic of the #status channel is updated with `vX.Y.Z is released! | ...`

## Major/minor releases and milestones

* Agave Science APIs releases follow [semver](https://semver.org/). Every release follows a major, minor, patch format (`vX.Y.Z`). 

* Security updates are applied as patch updates. 

* Updates for major and minor releases are delivered via maintenance releases (vX.Y.Z) and assigned to the corresponding open [GitHub milestone](https://github.com/agaveplatform/science-apis/milestones). That milestone remains open for the major/minor releases support lifetime, which ends once the milestone is closed. Then only a next major or minor release can be done.

* Agave Science APIs major and minor releases are bound to the given major/minor version numbers and other components' arbitrary versions, like MariaDB, MongoDB, Beanstalkd, and hosted third-party services. Older or newer component versions are not supported and not tested for the given release (even if referenced in the bundled Docker Compose files or Docker image build folders). 

* There are no unstable releases. Individual APIs are versioned as a single collection, and compatibility is only assured between services and components within the same release version. Breaking changes to databases and configurations will be described in the release notes. Other breaking changes, if introduced through external components, are considered out of Agave's scope and will be reported to the relevant component support channel.

* Minor releases can add or remove components as well as change components' versions, however in all such changes, a migration path will be provided. 

