from setuptools import setup, find_packages
setup(
    name = 'medallion_test_cg_3',
    version = '1.0',
    packages = find_packages(include = ('medallion_test_cg_3*', )) + ['prophecy_config_instances.medallion_test_cg_3'],
    package_dir = {'prophecy_config_instances.medallion_test_cg_3' : 'configs/resources/medallion_test_cg_3'},
    package_data = {'prophecy_config_instances.medallion_test_cg_3' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==2.1.5'],
    entry_points = {
'console_scripts' : [
'main = medallion_test_cg_3.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html', 'pytest-cov'], }
)
