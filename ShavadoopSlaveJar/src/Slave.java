import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Représente le travail à fournir par un slave.
 * - Soit une opération de mapping:
 * - Soit une opération de reducing:
 *
 * @author x0s
 */
public class Slave {

	//Définition des chemins d'accès
	private final static String PROJECT_PATH = "/path/to/ShavadoopMasterJar/";
	private final static String FILE_PATH = PROJECT_PATH + "files/";
	private final static String SPLIT_DIRECTORY = "splits/";
	private final static String UM_DIRECTORY = "unsortedMaps/";
	private final static String SPLIT_PATH = FILE_PATH + SPLIT_DIRECTORY;
	private final static String UM_PATH = FILE_PATH + UM_DIRECTORY;

	public static void main(String[] args) {

		//On mesure le temps d'execution brute
		long startTime = System.nanoTime();

		if(args.length >= 2)
		{
			if(args[0].equals("modeSXUMX")) //Mapping
			{
				String Sx = args[1];
				System.out.println("[Slave][Mapping] Je vais mapper le fichier:" + Sx);

				mapping(Sx);
			}
			else if(args[0].equals("modeUMXRMX")) //Reducing
			{
				String wordToReduce = args[1];
				String RMx = args[2];
				String[] UMx = Arrays.copyOfRange(args, 3, args.length);

				System.out.println("[Slave][Reducing] Je vais reduce les fichiers UMx");

				reduce(UMx, RMx, wordToReduce);
			}
			else
			{
				System.out.println("[Slave] Paramètre Mode inconnu!" + args[0] + " et " + args[1]);
			}
		}
		else
		{
			System.out.println("[Slave][Mapping] Aucun argument transmis. Pas de fichiers Splits à traiter ..feel useless..:(");
		}

		long endTime = System.nanoTime();
		long duration = (endTime - startTime);
		System.out.println("[Slave]Fin du calcul! Durée d'exécution: " + duration);
	}

	/**
	 * Cherche et compte les occurences d'un mot dans les fichiers UMx le contenant au moins une fois
	 *
	 * @param UMxArray		Tableau contenant la liste des fichiers UMx à parcourir
	 * @param RMx			Fichier Reduced Maps dans lequel écrire le résultat du reduce. (Fonctionnalité pas implémentée, on utilise la sortie standard pour renvoyer le résultat au master)
	 * @param wordToReduce	Mot dont on compte les occurences
	 */
	public static void reduce(String[] UMxArray, String RMx, String wordToReduce)
	{
		int count = 0;
		try {
			//On parcourt tous les fichiers UMx indiqués afin de trouver les occurences du mot à réduire.
			for(String UMx: UMxArray)
			{
				String UnsortedMapFilePath = UM_PATH + UMx;

				BufferedReader lecteurFichierUMx = new BufferedReader(new FileReader(UnsortedMapFilePath));
				String ligne;
				while((ligne = lecteurFichierUMx.readLine()) != null)
				{
					String[] words  = ligne.trim().split("\\s+");
					//System.out.println("On regarde si le mot est détecté" + ligne);
					if(words[0].equals(wordToReduce))
					{
						count++;
					}
				}
				lecteurFichierUMx.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally
		{
			System.out.println("[REDUCEDWORDS4MASTER_BEGIN]");
			System.out.println(wordToReduce + " " + Integer.toString(count));
			System.out.println("[REDUCEDWORDS4MASTER_END]");
		}
	}

	/**
	 * Détecte les mots distincts dans un fichier Split (Sx) et les transmet au master
	 * Génère les fichiers UMx(Unsorted Maps)
	 *
	 * @param Sx	Nom du fichier Split Sx à analyser
	 */
	public static void mapping(String Sx)
	{
		try {
			String splitFilePath = SPLIT_PATH + Sx;
			String UMx = Sx.replace("S", "UM");
			String UnsortedMapFilePath = UM_PATH + UMx;

			BufferedReader lecteurFichierSplit = new BufferedReader(new FileReader(splitFilePath));
			String ligne = lecteurFichierSplit.readLine();
			if(ligne != null)
			{

				String[] words  = ligne.split("\\s+");

				// On renvoie via le stream au master la liste des mots uniques traités
				String uniqueWords = getUniqueWords(words);
				System.out.println("[UNIQUEWORDS4MASTER_BEGIN]");
				System.out.println(uniqueWords);
				//ajoutTempsExecution(5);
				System.out.println("[UNIQUEWORDS4MASTER_END]");
				//On génère le fichier UMx (Unsorted Map x)
				File UnsortedMapFile = new File(UnsortedMapFilePath.trim());
				PrintWriter UMwriter = new PrintWriter(new BufferedWriter(new FileWriter(UnsortedMapFile)));
				for(String word : words)
				{
					UMwriter.write(word + " 1\n");
				}
				UMwriter.close();
			}
			lecteurFichierSplit.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally
		{
		}
	}

	/**
	 * A partir d'un tableau de mots, renvoie les mots distincts(uniques) concaténés dans une chaine.
	 *
	 * @param words	Tableau de chaines de caractères contenant des mots avec potentiellement de la redondance
	 * @return		Chaîne de caractère contenant les mots uniques séparés par un espace. Ex: "Motunique1 Motunique2 Motunique3"
	 */
	public static String getUniqueWords(String[] words)
	{
		HashSet<String> uniqueWordsSet = new HashSet<String>(Arrays.asList(words));
		String uniqueWords = "";

		for(String uniqueWord : uniqueWordsSet)
		{
			uniqueWords = uniqueWords + " " + uniqueWord;
		}
		return uniqueWords.trim();
	}

	/**
	 * Met en pause le thread courant pendant un certain temps en secondes
	 *
	 * @param secondes	Entier correspondant au nombre de secondes pendant lesquelles mettre en pause le thread
	 */
	public static void ajoutTempsExecution(int secondes)
	{
		try
		{
			Thread.sleep(secondes*1000);   //On allonge pour simuler un calcul plus gros et se rendre compte de la parallelisation
		}
		catch(InterruptedException ex)
		{
			Thread.currentThread().interrupt();
		}
	}

}
