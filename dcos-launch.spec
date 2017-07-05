# Warning:
#   - hidden import must be used as there is a bug in pyinstaller
#     https://github.com/pyinstaller/pyinstaller/issues/2185
#   - data must be decalared explicitly if not a .py file
#   - Building will suck up the local SSL .so and package it
#     with the final exe. Ensure build system has OpenSSL 1.0.2g or greater
a = Analysis(['dcos_launch/cli.py'],
             hiddenimports=['html.parser'],
             datas=[('dcos_test_utils/ip-detect/aws.sh', 'dcos_test_utils/ip-detect'),
                    ('dcos_test_utils/ip-detect/aws_public.sh', 'dcos_test_utils/ip-detect'),
                    ('dcos_test_utils/ip-detect/gce.sh', 'dcos_test_utils/ip-detect'),
                    ('dcos_test_utils/ip-detect/gce_public.sh', 'dcos_test_utils/ip-detect'),
                    ('dcos_test_utils/templates/*.json','dcos_test_utils/templates')
])
pyz = PYZ(a.pure, a.zipped_data, cipher=None)
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='dcos-launch',
    debug=False,
    strip=False,
    upx=True,
    console=True)
