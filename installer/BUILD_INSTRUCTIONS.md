# Building the StatArb Pro Installer

This guide explains how to create the Windows installer for StatArb Pro.

## Prerequisites

### 1. Python 3.9+
Download from: https://www.python.org/downloads/

**Important:** Check "Add Python to PATH" during installation.

### 2. Inno Setup 6.x
Download from: https://jrsoftware.org/isdl.php

Install with default options.

## Build Steps

### Quick Build (Automated)

1. Open Command Prompt
2. Navigate to the installer directory:
   ```
   cd C:\path\to\FIX_Protocol_Stat_Arb1\installer
   ```
3. Run the build script:
   ```
   build.bat
   ```
4. Find your installer in: `installer\output\StatArbPro_Setup_1.0.0.exe`

### Manual Build

If the automated build fails, follow these steps:

#### Step 1: Install Dependencies
```cmd
pip install pyinstaller
pip install -r ..\requirements.txt
```

#### Step 2: Build Executable
```cmd
pyinstaller statarb.spec --noconfirm --clean
```

#### Step 3: Create Installer
Open `setup.iss` in Inno Setup Compiler and click Build → Compile.

## Customization

### Changing the Icon

Replace `assets\icon.ico` with your own icon file.
- Size: 256x256 pixels recommended
- Format: ICO with multiple sizes (16, 32, 48, 256)
- Tool: Use https://convertico.com/ to convert PNG to ICO

### Changing the Version

Edit these files:
1. `setup.iss` - Line 10: `#define MyAppVersion "1.0.0"`
2. `version_info.txt` - Update `filevers` and `prodvers`
3. `launcher.py` - Update version string in splash screen

### Custom Branding

1. **Splash Screen Colors**: Edit `launcher.py`, look for:
   ```python
   bg_color = '#1a1a2e'
   accent_color = '#e94560'
   ```

2. **Installer Images**: Replace in `assets\`:
   - `wizard_large.bmp` - 164×314 pixels
   - `wizard_small.bmp` - 55×55 pixels

## Troubleshooting

### "Python not found"
Reinstall Python and ensure "Add to PATH" is checked.

### "pip not found"
Run: `python -m ensurepip --upgrade`

### PyInstaller errors
- Clear cache: Delete `build\` and `dist\` folders
- Update PyInstaller: `pip install --upgrade pyinstaller`

### Inno Setup errors
- Ensure PyInstaller completed successfully first
- Check that `dist\StatArbPro\` folder exists

## Output Files

After successful build:

```
installer/
├── dist/
│   └── StatArbPro/           # Standalone executable folder
│       ├── StatArbPro.exe    # Main application
│       └── (supporting files)
└── output/
    └── StatArbPro_Setup_1.0.0.exe  # Installer for distribution
```

## Distribution

The installer file (`StatArbPro_Setup_1.0.0.exe`) is self-contained and can be:
- Shared via email
- Uploaded to website
- Distributed via USB drive

Users just need to double-click to install - no Python required!
