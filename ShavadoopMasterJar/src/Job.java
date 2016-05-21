import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * Représente le travail d'un MapReduce
 * @author x0s
 */
public class Job {

	private final static String PROJECT_PATH = "/path/to/ShavadoopMasterJar/";

	// Fichier Machines
	private final static String LISTE_MACHINES_OPERATIONNELLES_PATH = PROJECT_PATH + "src/listeDesMachinesSucces.txt";
	private final static String LISTE_MACHINES_PATH = PROJECT_PATH + "src/listeDesMachines.txt";
	// Splits
	private final static String FILE_PATH = PROJECT_PATH + "files/";
	private final static String SPLIT_DIRECTORY = "splits/";
	private final static String SPLIT_PREFIX = "S";
	private final static String SPLIT_PATHPREFIXED = FILE_PATH + SPLIT_DIRECTORY + SPLIT_PREFIX;

	private UMx_dict UMX_dict;//dictionnaire mot->fichiersUMx les contenant
	private String name;

	/**
	 * Constructeur: initialise les attributs. Il est caractérisé par un nom et un dictionnaire UMX_dict
	 * @param name	String	nom du Job
	 */
	public Job(String name)
	{
		this.name = name;
		this.UMX_dict = new UMx_dict();

	}

	/**
	 * Lance un Job de MapReduce.
	 * Peut aussi lancer une routine de détection des machines disponibles
	 *
	 * @param inputFileSrcPath	String	Chemin du fichier d'Input dans le projet(Ex: "src/input.txt")
	 */
	public void launchJob(String inputFileSrcPath)
	{
		//detecterLesMachinesDisponibles("133"); //Décommenter pour activer

		String inputFilePath = PROJECT_PATH + inputFileSrcPath;

		//Input Splitting: Un fichier texte --> Des fichiers splits blocs de k ligne=Sx
		int nbInputSplits = inputSplitting(inputFilePath);
		System.out.println("[Master][InputSplitting] Nombre d\'input Splits à traiter:" + nbInputSplits);

		//Split Mapping: Parallelisation. Pour chaque Sx(inputSplit) --> Unsorted Map(Umx) sur une machine
		System.out.println("[Master][SplitMapping] SplitMapping initié");
		this.splitMapping(nbInputSplits);

		// Reducing sorted maps: Parallelisation. UMx -> SMx + SMx -> RMx
		System.out.println("[Master][Reducing] Reducing initié");
		this.reducing();

		// Agrégation des RMx pour former le fichier output

	}
	/**
	 * Detecte les machines disponibles dans une salle donnée et met à jour le fichier listeDesMachinesSucces.txt
	 *
	 * @param salle	String	nom de la salle dans laquelle parcourir les machines
	 */
	public void detecterLesMachinesDisponibles(String salle)
	{
		this.resetListeMachinesFile();
		this.genererFichierMachines(salle);
		this.resetListeMachinesSuccesFile();
		this.rechercheLesMachinesOperationnelles();
	}


	/**
	 * A partir du dictionnaire word-UMx, cette méthode va lancer les traitements de reduce en répartissant la charge sur les slaves disponible
	 */
	public void reducing()
	{
		HashMap<String, ArrayList<String>> WordsUmxDict = this.UMX_dict.getWords_UMx(); // on récupère le dictionnaire
		//pour chaque clef, lancer en // le reducing sorted maps

	    // On récupère la liste des arguments UMx à transmettre aux slaves
	    HashMap<String, String> commandsArgsForReducing = getCommandsArgsForReducing(WordsUmxDict);
	    int nbReducedMaps = commandsArgsForReducing.size();

		ArrayList<String> machinesDispo = getMachinesDispos();

        int nbMachinesDispos = machinesDispo.size();
        int indexFirstReduceFile = 0;
        int indexLastReduceFile = 0;
        int nbReduceFilesForThisWave = nbMachinesDispos; //Le nombre de splitFile/wave sera égal au maximum des capacités sauf (peut-être) à la dernière wave
        int nbReducingWaves = (int) Math.ceil( (double) nbReducedMaps/nbMachinesDispos );
        System.out.println("[Master][Reducing] nbMappingWaves = " + nbReducingWaves);
        System.out.println("[Master][Reducing] nbMachinesDispos = " + nbMachinesDispos);

        for (int reducingWave = 1; reducingWave <= nbReducingWaves; reducingWave++)
        {
        	indexFirstReduceFile	= ( reducingWave - 1) * nbMachinesDispos;
        	if(reducingWave == nbReducingWaves) //Si on est à la dernière wave
        	{
        		int nbReducedMapsMaxTheorique = indexLastReduceFile + nbMachinesDispos;
        		if( ( nbReducedMapsMaxTheorique - nbReducedMaps) > 0)
        		{// Si on dépasse le nombre d'InputSplits, on ramène à la valeur réelle.
        			nbReduceFilesForThisWave	= nbReducedMaps - indexLastReduceFile;
        		}
        	}
        	indexLastReduceFile	= indexFirstReduceFile + nbReduceFilesForThisWave;

			ArrayList<String> machinesListForThisWave = new ArrayList<String>(machinesDispo.subList(0, nbReduceFilesForThisWave)); //Il y a au plus 1 fichier à traiter pour chaque machine

			System.out.println("[Master][Reducing] idxFirst = " + indexFirstReduceFile + " / idxLast = " + indexLastReduceFile);
			System.out.println("[Master][Reducing] nbReducedMaps = " + nbReducedMaps);
			System.out.println("[Master][Reducing] nbReduceFilesForThisWave = " + nbReduceFilesForThisWave);
			System.out.println("[Master][Reducing] Reducing Wave [" + reducingWave + "] sollicitant ["+ machinesListForThisWave.size() +"] reducers");

			//Pour chaque inputSplit, on lance une opération de mapping sur un ensemble de machines
			this.launchReducingWave(machinesListForThisWave, indexFirstReduceFile, commandsArgsForReducing);
		}


	}
	/**
	 * Lance un nombre d'opérations de Reduce selon la répartition de charges définie par reducing.
	 * Ce nombre peut être saturé en nombre de machines disponibles ou non selon le besoin.
	 *
	 * @param machinesListForThisWave 	ArrayList<String> 		Liste des machines sur lesquelles lancer les opération de reduce
	 * @param indexFirstReduceFile		int						Indice du mot à traiter dans la pile
	 * @param commandsArgsForReducing	HashMap<String, String> HashMap des mots et de leurs UMx associés (concaténés ensemble et séparés par un espace)
	 * @return	boolean
	 * @see #reducing()
	 * @see #connexionUneMachine(String, String, boolean, String)
	 */
	public boolean launchReducingWave(ArrayList<String> machinesListForThisWave, int indexFirstReduceFile, HashMap<String, String> commandsArgsForReducing)
	{
		ArrayList<Thread> ThreadStack = new ArrayList<Thread>();
		Thread ThreadEncours = new Thread();

        String commandeSlaveJar = "java -jar ~/workspace/P1_Shavadoop/ShavadoopSlaveJar/src/SlaveJar.jar modeUMXRMX";

        ArrayList<String> commandsArgsWords = new ArrayList<String>(commandsArgsForReducing.keySet());
        ArrayList<String> commandsArgsUMx   = new ArrayList<String>(commandsArgsForReducing.values());

        for (int i = 0; i < machinesListForThisWave.size(); i++)
        {
        	int currentIndex = indexFirstReduceFile + i;
        	String commandeSlaveJarWithArgs = String.format(commandeSlaveJar + " " + commandsArgsWords.get(currentIndex) + " RM%d %s", currentIndex, commandsArgsUMx.get(currentIndex));
        	//String indexRMxFileSlave = Integer.toString((indexFirstReduceFile + i));
        	if ((ThreadEncours = connexionUneMachine(machinesListForThisWave.get(i), commandeSlaveJarWithArgs, false, "")) != null)
    		{
    			ThreadStack.add(ThreadEncours); // On récupère le Thread initié Ne faudrait-il pas écrire Thread.currentThread() ?
    		}
    	}
        // On signifie au Thread principal (celui en cours qui va executer la boucle for) d'attendre la fin d'execution de tous les Threads initiés précédemment
        for(Thread ThreadLanceAilleurs : ThreadStack)
        {
        	try {
				ThreadLanceAilleurs.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }

        System.out.println("[Master][Reducing] Tout est fini! Lancement d'un slaveJar sur " + ThreadStack.size() +" machine(s) operationnelle(s)");


		return false;
	}

	/**
	 * Récupère la liste des fichiers UMx à partir de l'attribut d'un objet UMx_dict et les renvoie sous une forme sérialisée facilitant le passage en argument
	 * format: <mot, chaine concaténée> = <"Car", "UM2 UM7 UM9">
	 *
	 * @param WordsUmxDict 	HashMap<String, ArrayList<String>>  HashMap contenant les mots associés à une arrayList contenant les fichiers UMx dans lesquels ils se trouvent
	 * @return 				HashMap<String, String>				HashMap contenant les mots associés à une châine contenant la liste des fichiers UMx concaténée
	 */
	public HashMap<String, String> getCommandsArgsForReducing(HashMap<String, ArrayList<String>> WordsUmxDict)
	{
		HashMap<String, String> commandsArgsForReducing = new HashMap<String, String>();
		Iterator<Entry<String, ArrayList<String>>> iter = WordsUmxDict.entrySet().iterator();

	    while (iter.hasNext()) {
	        Entry<String, ArrayList<String>> entry = iter.next();

	        List<String> UMxListe = new ArrayList<String>();

	        String clef = entry.getKey(); //Une clef est un mot à traiter
	        UMxListe = entry.getValue();
	        String currentCommandArgs = "";
	        for (int i = 1; i <= UMxListe.size(); i++){

	            currentCommandArgs = currentCommandArgs + " " + UMxListe.get(i-1);
	        }
	        commandsArgsForReducing.put(clef, currentCommandArgs);
	    }
	    return commandsArgsForReducing;
	}

	/**
	 * Cette méthode va répartir la charge de traitement des fichiers Splits sur les différents mappers(machines) disponibles .
	 * Elle va donc lancer des vagues de mapping sur chaque machine disponible (une machine = un mapping(un/plusieurs fichiers inputSplits))
	 *
	 * @param nbInputSplits	int	nombre d'inputSplits à aller chercher dans le dossier dédié du master
	 */
	public void splitMapping(int nbInputSplits)
	{
		ArrayList<String> machinesDispo = getMachinesDispos();

        int nbMachinesDispos = machinesDispo.size();
        int indexFirstSplitFile = 0;
        int indexLastSplitFile = 0;
        int nbSplitFilesForThisWave = nbMachinesDispos; //Le nombre de splitFile/wave sera égal au maximum des capacités sauf (peut-être) à la dernière wave
        int nbMappingWaves = (int) Math.ceil( (double) nbInputSplits/nbMachinesDispos );
        System.out.println("[Master][splitMapping] nbMappingWaves = " + nbMappingWaves);
        System.out.println("[Master][splitMapping] nbMachinesDispos = " + nbMachinesDispos);

        for (int mappingWave = 1; mappingWave <= nbMappingWaves; mappingWave++)
        {
        	indexFirstSplitFile	= ( mappingWave - 1) * nbMachinesDispos;
        	if(mappingWave == nbMappingWaves) //Si on est à la dernière wave
        	{
        		int nbInputSplitsMaxTheorique = indexLastSplitFile + nbMachinesDispos;
        		if( ( nbInputSplitsMaxTheorique - nbInputSplits) > 0)
        		{// Si on dépasse le nombre d'InputSplits, on ramène à la valeur réelle.
        			nbSplitFilesForThisWave	= nbInputSplits - indexLastSplitFile;
        		}
        	}
        	indexLastSplitFile	= indexFirstSplitFile + nbSplitFilesForThisWave;

			ArrayList<String> machinesListForThisWave = new ArrayList<String>(machinesDispo.subList(0, nbSplitFilesForThisWave)); //Il y a au plus 1 fichier à traiter pour chaque machine

			System.out.println("[Master][splitMapping] idxFirst = " + indexFirstSplitFile + " / idxLast = " + indexLastSplitFile);
			System.out.println("[Master][splitMapping] nbInputSplits = " + nbInputSplits);
			System.out.println("[Master][splitMapping] nbSplitFilesForThisWave = " + nbSplitFilesForThisWave);
			System.out.println("[Master][splitMapping] Mapping Wave [" + mappingWave + "] sollicitant ["+ machinesListForThisWave.size() +"] mappers");

			//Pour chaque inputSplit, on lance une opération de mapping sur un ensemble de machines
			this.launchMappingWave(machinesListForThisWave, indexFirstSplitFile,indexLastSplitFile);
		}
	}

	/**
	 * Cette méthode va éxécuter sur chacune des machines disponible ET nécéssaire, une opération de mapping(extraction des mots de l'inputSplit>Clés + Comptage>1)
	 * Elle va exécuter un slaveMap.jar à distance sur les machines autant de fois que de fichiers. Donc potentiellement plus que le nombre de mappers
	 *
	 * @param machinesListForThisWave 	ArrayList<String>	Liste des machines sur lesquelles lancer les opération de mapping
	 * @param indexFirstSplitFile		int					Indice du premier Split à traiter dans cette vague
	 * @param indexLastSplitFile		int					Indice du dernier Split à traiter + 1
	 */
	public void launchMappingWave(ArrayList<String> machinesListForThisWave, int indexFirstSplitFile,int indexLastSplitFile)
	{

		ArrayList<Thread> ThreadStack = new ArrayList<Thread>();
		Thread ThreadEncours = new Thread();

        String commandeSlaveJar = "java -jar ~/workspace/P1_Shavadoop/ShavadoopSlaveJar/src/SlaveJar.jar modeSXUMX";

        for (int i = 0; i < machinesListForThisWave.size(); i++)
        {
        	String commandeSlaveJarWithArgs = String.format(commandeSlaveJar + " S%d", (indexFirstSplitFile + i));
        	String indexSxUmxFileSlave = Integer.toString((indexFirstSplitFile + i));
        	if ((ThreadEncours = connexionUneMachine(machinesListForThisWave.get(i), commandeSlaveJarWithArgs, false, indexSxUmxFileSlave)) != null)
    		{
    			ThreadStack.add(ThreadEncours); // On récupère le Thread initié Ne faudrait-il pas écrire Thread.currentThread() ?
    		}
    	}
        // On signifie au Thread principal (celui en cours qui va executer la boucle for) d'attendre la fin d'execution de tous les Threads initiés précédemment
        for(Thread ThreadLanceAilleurs : ThreadStack)
        {
        	try {
				ThreadLanceAilleurs.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }

        System.out.println("[Master][Mapping]Tout est fini! Lancement d'un slaveJar sur " + ThreadStack.size() +" machine(s) operationnelle(s)");
	}

	/**
	 * Renvoie la liste des machines disponibles sur lesquelles il est possible d'exécuter des commandes à distance.
	 *
	 * @return ArrayList<String> liste des machines disponibles
	 */
	public static ArrayList<String> getMachinesDispos()
    {
		ArrayList<String> machinesDispo = new ArrayList<String>();
		BufferedReader lecteurMachinesDispos;
		try {
			lecteurMachinesDispos = new BufferedReader(new FileReader(LISTE_MACHINES_OPERATIONNELLES_PATH));

			String uneMachine;
			while ((uneMachine = lecteurMachinesDispos.readLine()) != null)
			{
				machinesDispo.add(uneMachine);
			}
			lecteurMachinesDispos.close();
		}
	    catch (IOException e)
		{
			e.printStackTrace();
		}

	    return machinesDispo;
    }

	/**
	 * Cette méthode découpe un fichier d'input en d'autres fichiers (les splits) qui serviront en entrée à chacun des mappers.
	 * On choisit comme unité de découpe, le nombre de lignes.
	 *
	 * PS: On peut faire varier la taille des fichiers Sx (Split)
	 * splitFileSize: + grand = - de fichiers à traiter
	 *
	 * @param inputFilePath		String	Fichier input à lire
	 * @return					int		Renvoie le nombre de fichiers Splits (x+1 for Sx=Smax)
	 */
	@SuppressWarnings("finally")
	public static int inputSplitting(String inputFilePath)
	{

		int splitFileSize = 2; // taille du fichier Sx (Split x) en nombre de lignes
		int nbLignesParcourues = 0;
		int splitIndex = 0; // indice du fichier Split dans lequel écrire
		try
		{
			//On récupère les lignes du fichier d'input pour les écrire progressivement dans des fichiers Splits (Sx)
			BufferedReader lecteurInputLines = new BufferedReader(new FileReader(inputFilePath));
		    String line;
		    FileWriter splitWriter = new FileWriter(SPLIT_PATHPREFIXED + "0"); //Premier fichier dans lequel écrire

		    while((line = lecteurInputLines.readLine())!=null)
		    {
		    	System.out.println(line); // Q25. vérifier dans un terminal avec: "java -jar ShavadoopMasterJar.jar input.txt"
		    	splitWriter.write(line+" ");
		    	if ( (nbLignesParcourues+1)%splitFileSize == 0 && nbLignesParcourues != 0) //Si on atteint le nombre de lignes correspondant à la taille de fichier souhaité
		    	{
		    		splitWriter.close();//On termine l'écriture du précédent fichier.
		    		splitIndex = (nbLignesParcourues+1) / splitFileSize; //le reste étant nul, l'index est bien un (int)
		    		//On instantie un nouveau flux de fichier
		    		splitWriter = new FileWriter(SPLIT_PATHPREFIXED + splitIndex); //S0, S1, S2...
		    	}
		    	nbLignesParcourues += 1;
		    }
		    //On ferme les flux de fichier
		    splitWriter.close();
		    lecteurInputLines.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			return (splitIndex+1);
		}

	}

	/**
	 * Supprime le fichier contenant la liste des machines opérationnelles s'il existe
	 * Créé un nouveau fichier pour acceuillir la nouvelle liste
	 */
	public void resetListeMachinesSuccesFile()
	{
		try
        {
	        File file = new File(LISTE_MACHINES_OPERATIONNELLES_PATH);
			if (file.exists())
			{
				file.delete();
				file.createNewFile();
			}
        }
		catch(Exception e)
		{
	        e.printStackTrace();
	     }
	}

	/**
	 * Supprime le fichier contenant la liste des machines existantes si ce fichier existe
	 * Créé un nouveau fichier pour acceuillir la nouvelle liste
	 */
	public void resetListeMachinesFile()
	{
		try
        {
	        File file = new File(LISTE_MACHINES_PATH);
			if (file.exists())
			{
				file.delete();
				file.createNewFile();
			}
        }
		catch(Exception e)
		{
	        e.printStackTrace();
	     }
	}

	/**
	 * Cette méthode teste les machines en leur demandant de répondre positivement à une exécution
	 * d'une commande de test (test si un echo $((2+3)) est réalisé à distance)
	 * PS: L'écriture du fichier contenant les machines opérationnelles est effectuée à l'éxécution de Run dans afficheurFlux
	 *
	 * @return String	liste des machines Opérationnelles
	 */
	protected String rechercheLesMachinesOperationnelles()
	{
		String listeMachinesOK 			 = "";
		String uneMachine 				 = "";
		BufferedReader lecteurAvecBuffer = null;
		String commande 				 = "java -jar ~/workspace/P1_Shavadoop/ShavaPremierJar/ShavaPremierJar.jar"; //exécution d'un echo $((2+3))
		try
		{
            lecteurAvecBuffer = new BufferedReader(new FileReader(LISTE_MACHINES_PATH));
            while ((uneMachine = lecteurAvecBuffer.readLine()) != null)
        	{
            	//une ligne = une machine (ex: c130-33)
        		//System.out.println(uneMachine);
        		if (this.connexionUneMachine(uneMachine, commande, true, "") != null)
        		{ //Si la connexion a reussi, on stocke le nom de la machine, true=mode test activé
        			listeMachinesOK = listeMachinesOK + uneMachine + "\n";
        		}
        	}
            lecteurAvecBuffer.close();
        }
        catch(FileNotFoundException exc)
        {
            	System.out.println("Erreur d'ouverture");
        }
		catch (IOException e)
		{
            System.out.println("exception happened - here's what I know: ");
            e.printStackTrace();
            System.exit(-1);
		}
		return listeMachinesOK;

	}

	/**
	 * Démarre un Thread dédié à l'exécution d'une commande sur une machine distante
	 *
	 * @param nomMachine		String	Nom de la machine sur laquelle exécuter la commande
	 * @param commande			String	Commande à exécuter
	 * @param testMode			Boolean Mode de détection des machines disponibles (true si activé)
	 * @param indexFileToTreat	String  Indice du fichier à traiter. Uniquement dans le cas du Mapping
	 * @return					Thread	Thread démarré
	 */
    protected Thread connexionUneMachine(String nomMachine, String commande, boolean testMode, String indexFileToTreat)
    {

    	try
    	{
    		String commandeShell = "ssh " + nomMachine + " " + commande;
	        Process p = new ProcessBuilder("sh", "-c",commandeShell).start();

	        AfficheurFlux fluxSortie = new AfficheurFlux(p.getInputStream(), nomMachine, testMode, this.UMX_dict, indexFileToTreat);
	        AfficheurFlux fluxErreur = new AfficheurFlux(p.getErrorStream(), nomMachine, testMode, this.UMX_dict, indexFileToTreat);
	        Thread ThreadEnCours = new Thread(fluxSortie);
	        Thread ErrorThreadEnCours = new Thread(fluxErreur);

	        ThreadEnCours.start(); //on démarre le thread (execution de la commande)
	        ErrorThreadEnCours.start();

	        return ThreadEnCours; //Si on est arrivé jusqu'ici, connexion réussie, on renvoie le Thread pour dire au main Thread d'attendre

    	}
    	catch (IOException e)
    	{
	        System.out.println("exception happened - here's what I know: ");
	        e.printStackTrace();
	        System.exit(-1);
    	}
    	return null;
    }

    /**
     * Génère un fichier contenant ligne par ligne le nom des machines candidates dans une salle
     *
     * @param salle	String	numéro de la salle. ex: "127","128"..."133"
     */
	protected void genererFichierMachines(String salle)
	{

			String content = "";
			for (int i =1; i < 33; i++)
			{
				String zero = "";
				if (i<10) {
					zero = "0";
				}
				content = content + "c"+ salle + "-" + zero + i +"\n"; //ex: c133-17
			}

			ecrireFichierTexte(content, LISTE_MACHINES_PATH);

	}

	/**
	 * Ecrit une ligne dans un fichier texte
	 *
	 * @param contenu	String	Chaîne de caractère à écrire
	 * @param chemin	String	Chemin du fichier à créer
	 */
	public static void ecrireFichierTexte(String contenu, String chemin)
	{
		try {

			File file = new File(chemin);

			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			bw.write(contenu);
			bw.close();

			System.out.println("[Master] Ecriture Terminée! " + chemin);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Getter pour récupérer le dictionnaire
	 *
	 * @return UMx_dict Dictionnaire des mots-UMx-count
	 */
	public UMx_dict getUMX_dict()
	{
		return this.UMX_dict;
	}

	/**
	 * Getter pour récupérer la valeur de l'attribut name
	 *
	 * @return		String Nom du Job
	 */
	public String getName()
	{
		return this.name;
	}

}
