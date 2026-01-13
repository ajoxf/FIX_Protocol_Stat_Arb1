; Inno Setup Script for StatArb Pro
; Professional Windows Installer
;
; Requirements:
;   - Inno Setup 6.x (https://jrsoftware.org/isinfo.php)
;   - Run PyInstaller first to create dist/StatArbPro folder
;
; Build: Open this file in Inno Setup Compiler and click Build

#define MyAppName "StatArb Pro"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "StatArb Pro"
#define MyAppURL "https://github.com/ajoxf/FIX_Protocol_Stat_Arb1"
#define MyAppExeName "StatArbPro.exe"
#define MyAppAssocName "StatArb Pro Project"
#define MyAppAssocExt ".sarb"
#define MyAppAssocKey StringChange(MyAppAssocName, " ", "") + MyAppAssocExt

[Setup]
; Application Information
AppId={{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}

; Installation Settings
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
AllowNoIcons=yes

; Output Settings
OutputDir=output
OutputBaseFilename=StatArbPro_Setup_{#MyAppVersion}
SetupIconFile=assets\icon.ico
UninstallDisplayIcon={app}\{#MyAppExeName}

; Compression
Compression=lzma2/ultra64
SolidCompression=yes
LZMAUseSeparateProcess=yes

; Privileges (per-user install by default, no admin needed)
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog

; Visual Style
WizardStyle=modern
WizardSizePercent=120
WizardImageFile=assets\wizard_large.bmp
WizardSmallImageFile=assets\wizard_small.bmp

; Minimum Windows version
MinVersion=10.0

; License
LicenseFile=assets\license.rtf
InfoBeforeFile=assets\readme.rtf

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "quicklaunchicon"; Description: "{cm:CreateQuickLaunchIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked; OnlyBelowVersion: 6.1; Check: not IsAdminInstallMode

[Files]
; Main application files (from PyInstaller output)
Source: "dist\StatArbPro\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

; Additional documentation
Source: "assets\Quick_Start_Guide.pdf"; DestDir: "{app}\docs"; Flags: ignoreversion

[Icons]
; Start Menu
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\Quick Start Guide"; Filename: "{app}\docs\Quick_Start_Guide.pdf"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"

; Desktop (optional)
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

; Quick Launch (optional, older Windows)
Name: "{userappdata}\Microsoft\Internet Explorer\Quick Launch\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: quicklaunchicon

[Run]
; Launch after installation
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[Registry]
; File association (optional)
Root: HKA; Subkey: "Software\Classes\{#MyAppAssocExt}\OpenWithProgids"; ValueType: string; ValueName: "{#MyAppAssocKey}"; ValueData: ""; Flags: uninsdeletevalue
Root: HKA; Subkey: "Software\Classes\{#MyAppAssocKey}"; ValueType: string; ValueName: ""; ValueData: "{#MyAppAssocName}"; Flags: uninsdeletekey
Root: HKA; Subkey: "Software\Classes\{#MyAppAssocKey}\DefaultIcon"; ValueType: string; ValueName: ""; ValueData: "{app}\{#MyAppExeName},0"
Root: HKA; Subkey: "Software\Classes\{#MyAppAssocKey}\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#MyAppExeName}"" ""%1"""
Root: HKA; Subkey: "Software\Classes\Applications\{#MyAppExeName}\SupportedTypes"; ValueType: string; ValueName: ".sarb"; ValueData: ""

[Code]
// Custom Pascal code for installer logic

var
  WelcomeLabel: TLabel;

// Check if .NET Framework or other dependencies are installed
function IsDependencyInstalled(): Boolean;
begin
  // Always return true for now, add checks if needed
  Result := True;
end;

// Custom welcome page text
procedure InitializeWizard;
begin
  // Customize welcome page
  WizardForm.WelcomeLabel2.Caption :=
    'This will install StatArb Pro on your computer.' + #13#10 + #13#10 +
    'StatArb Pro is a professional multi-broker statistical arbitrage trading system ' +
    'supporting MT5, FIX Protocol, FlexTrade, and Interactive Brokers.' + #13#10 + #13#10 +
    'Features:' + #13#10 +
    '  • Real-time Z-score monitoring' + #13#10 +
    '  • Hurst exponent regime detection' + #13#10 +
    '  • Synchronized multi-broker execution' + #13#10 +
    '  • Professional web-based interface' + #13#10 + #13#10 +
    'Click Next to continue, or Cancel to exit Setup.';
end;

// Show a custom message on finish
procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    // Any post-install actions
  end;
end;

// Cleanup on uninstall
procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usPostUninstall then
  begin
    // Remove user data directory if empty
    RemoveDir(ExpandConstant('{userappdata}\StatArbPro'));
  end;
end;
