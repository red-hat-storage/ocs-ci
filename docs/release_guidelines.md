# OCS-CI Releases

OCS-CI releases are generated in conjunction with OCS releases, A release is cut
as soon as OCS is Generally Available and available in open market place.

The release helps to checkout specific versions of tests that were run during
the release and serves as baseline for the specific version of OCS.

# Release Process
1. Update the setup.py to match the ocs version and commit the change.
2. Create a tag specific to commit that matches the latest ocs-ci that was used to run
the regression tests. one can create the tag via the git command line or via github
  ### CLI:
       * `git tag -l` to list current tags
       * `git tag -a v4.5.0 abceb01 -m "v4.5.0 tag"` to add tag to commit abceb01
       * `git push --tags` to push the tag
  ### Github GUI:
      From the main project, use the release page to Draft a new release
      Specify new tag name (eg: v4.5.1) and the commit in '@' target field
      Describe the release notes and publish the draft (steps 3 and 4 below)

3. Update the release notes to highlight changes that went in the ocs-ci repo
4. Generate the release