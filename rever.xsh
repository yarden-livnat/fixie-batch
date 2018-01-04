$PROJECT = $GITHUB_REPO = 'fixie-batch'
$GITHUB_ORG = 'ergs'

$ACTIVITIES = [
    #'pytest',  # requires new version of cyclus to be released
    'version_bump', 'changelog', 'tag', 'push_tag', 'pypi', 'ghrelease']

$VERSION_BUMP_PATTERNS = [
    ('setup.py', 'VERSION\s*=.*', "VERSION = '$VERSION'"),
    ('fixie_batch/__init__.py', '__version__\s*=.*', "__version__ = '$VERSION'"),
    ]
$CHANGELOG_FILENAME = 'CHANGELOG.rst'
$CHANGELOG_TEMPLATE = 'TEMPLATE.rst'

$DOCKER_CONDA_DEPS = ['fixie', 'pprintpp', 'pytest', 'pytest-tornado',
                      'jinja2', 'cyclus']
$DOCKER_INSTALL_COMMAND = 'git clean -fdx && ./setup.py install'

$CONDA_FORGE_SOURCE_URL = 'https://pypi.io/packages/source/f/$PROJECT/$PROJECT-$VERSION.tar.gz'
