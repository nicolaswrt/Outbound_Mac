import os
from pathlib import Path

def get_firefox_profile():
    """
    Findet das Firefox-Profil des Benutzers.
    
    Returns:
        str: Pfad zum Firefox-Profil oder None wenn nicht gefunden
    """
    profile_path = Path(os.getenv('APPDATA')) / 'Mozilla' / 'Firefox' / 'Profiles'
    
    try:
        # Suche nach default oder release Profil
        for profile in profile_path.glob('*.default*'):
            print(f"Gefundenes Profil: {profile}")
            return str(profile)
            
        # Falls kein default Profil gefunden wurde, suche nach anderen Profilen
        for profile in profile_path.glob('*'):
            if profile.is_dir():
                print(f"Alternatives Profil gefunden: {profile}")
                return str(profile)
                
    except Exception as e:
        print(f"Fehler beim Suchen des Firefox-Profils: {str(e)}")
    
    return None

def setup_driver_options(profile_path):
    """
    Erstellt die Firefox-Optionen mit dem gegebenen Profil.
    
    Args:
        profile_path (str): Pfad zum Firefox-Profil

    Returns:
        Options: Konfigurierte Firefox-Optionen
    """
    from selenium.webdriver.firefox.options import Options
    
    options = Options()
    options.add_argument('-profile')
    options.add_argument(profile_path)
    
    return options

def create_driver(profile_path):
    """
    Erstellt einen Firefox-Driver mit dem gegebenen Profil.
    
    Args:
        profile_path (str): Pfad zum Firefox-Profil

    Returns:
        WebDriver: Konfigurierter Firefox-Driver oder None bei Fehler
    """
    from selenium import webdriver
    from selenium.webdriver.firefox.service import Service
    
    try:
        options = setup_driver_options(profile_path)
        service = Service('geckodriver.exe')
        return webdriver.Firefox(service=service, options=options)
    except Exception as e:
        print(f"Fehler beim Erstellen des Firefox-Drivers: {str(e)}")
        return None

# Optional: Hilfsfunktionen für Excel-Export
def save_to_excel(df, filename):
    """
    Speichert DataFrame in Excel-Datei.
    
    Args:
        df (DataFrame): Zu speichernde Daten
        filename (str): Name der Excel-Datei
    """
    try:
        df.to_excel(filename, index=False)
        print(f"Daten erfolgreich in {filename} gespeichert")
        return True
    except Exception as e:
        print(f"Fehler beim Speichern der Excel-Datei: {str(e)}")
        return False

# Test-Funktion
if __name__ == "__main__":
    # Test der Funktionen
    profile = get_firefox_profile()
    if profile:
        print(f"Firefox-Profil gefunden: {profile}")
    else:
        print("Kein Firefox-Profil gefunden")
