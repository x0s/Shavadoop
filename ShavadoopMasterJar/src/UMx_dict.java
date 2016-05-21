
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
/**
 * Représente un dictionnaire de mots-(Fichiers Unsorted Maps + nombre d'occurences du mot)
 * 
 * @author x0s
 */
public class UMx_dict {
	
	private HashMap<String, ArrayList<String>> words_UMx;
	
	public UMx_dict()
	{
		this.words_UMx = new HashMap<String, ArrayList<String>>();
	}
	
	public HashMap<String, ArrayList<String>> getWords_UMx()
	{
		return this.words_UMx;
	}
	
	public ArrayList<String> getUMxFiles(String word)
	{
		return this.words_UMx.get(word);
	}
	
	/**
	 * Ajoute une référence à un fichier UMx dans le dictionnaire pour un mot donné
	 * 
	 * @param wordTreatedBySlave 	Mot présent dans le fichier UMx
	 * @param UMxFileIndex			Indice du fichier UMx contenant le mot wordTreatedBySlave
	 */
	public void addUMxFile(String wordTreatedBySlave, String UMxFileIndex)
	{
		ArrayList<String> UMXfiles;
		String FileNameContainingWord = "UM" + UMxFileIndex;
		if (this.words_UMx.containsKey(wordTreatedBySlave)) //si le mot est déjà référencé
		{
			UMXfiles = this.words_UMx.get(wordTreatedBySlave);
			UMXfiles.add(FileNameContainingWord);
		}
		else
		{
			UMXfiles = new ArrayList<String>(Arrays.asList(FileNameContainingWord));
		}
		System.out.println("UMX_dict" + UMxFileIndex + " > Ajout du mot:"+wordTreatedBySlave + " OK");
		this.words_UMx.put(wordTreatedBySlave, UMXfiles);
		
	}
	/**
	 * Ajoute dans le dictionnaire mots-UMX le nombre d'occurences d'un mot
	 * 
	 * @param word 	Mot dont on veut ajouter le nombre d'occurences
	 * @param count Nombres d'occurences comptées dans les UMx
	 */
	public void addWordCount(String word, String count)
	{
		ArrayList<String> UMXfiles;
		if (this.words_UMx.containsKey(word))
		{
			UMXfiles = this.words_UMx.get(word);
			UMXfiles.add(count); //On ajoute en tant que dernier élément de l'arraylist
		}
	}

}
