import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

/**
 * Permet de récupèrer les flux des machines (les sorties standard)
 * 
 * @author x0s
 */
class AfficheurFlux implements Runnable {

    private final InputStream inputStream;
    private boolean testMachinesDistantes = false;
    private String listeMachinesOperationnellesPath = "/cal/homes/gfidanza/workspace/P1_Shavadoop/ShavadoopMasterJar/src/listeDesMachinesSucces.txt";
    private String outputPath = "/cal/homes/gfidanza/workspace/P1_Shavadoop/ShavadoopMasterJar/src/output.txt";
    private String nomMachine;
    private String indexFileToTreat; //indice du fichier que traite le slave (7 pour S7/UM7)
    UMx_dict UMX_dict;
    
    /**
     * Constructeur. Initialisation des attributs de l'objet.
     * 
     * @param inputStream			Inpustream	Flux sur lequel aller lire la sortie de la machine
     * @param nomMachine			String		Nom de la machine correspondante
     * @param testMachinesDistantes Booléen		Active ou désactive le mode de test de disponibilité des machines distantes
     * @param UMX_dict				UMx_dict	Dictionnaire word-fichiers UMx
     * @param indexFileToTreat		String		
     */
    AfficheurFlux(InputStream inputStream, String nomMachine, boolean testMachinesDistantes, UMx_dict UMX_dict, String indexFileToTreat) 
    {
        this.inputStream 			= inputStream;
        this.nomMachine 			= nomMachine;
        this.testMachinesDistantes 	= testMachinesDistantes;
        this.UMX_dict 				= UMX_dict;
        this.indexFileToTreat 		= indexFileToTreat;
    }

    private BufferedReader getBufferedReader(InputStream is) 
    {
        return new BufferedReader(new InputStreamReader(is));
    }
    /**
     * On récupère ce qui transite sur le flux juste après l'exécution des Threads (commande ssh à distance).
     * Soit on analyse les sorties des machines afin de tester leurs disponibilités.
     * Soit on récupère les travaux des slaves après leurs travaux de Mapping ou de Reducing.
     */
    @Override
    public void run() 
    { 
        BufferedReader br = getBufferedReader(inputStream);
        String ligne = "";
        boolean SlaveCallBackMasterAfterMapping  = false;
        boolean SlaveCallBackMasterAfterReducing = false;
        try 
        {
            while ((ligne = br.readLine()) != null) //Tant qu'il y a qqch sur le flux de sortie de la machine
            { 
                if(this.testMachinesDistantes)//Si Mode Test des machines ON
                {
	            	if( (ligne.length() == 1) && (Integer.parseInt(ligne) == 5) ) // On teste les machines en vérifiant l'éxécution d'un echo $((2+3)) en sortie standard > "5"
	                {
	                	try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(this.listeMachinesOperationnellesPath, true)))) 
	                	{// On ajoute le nom de la machine dans le fichier listeDesMachinesSucces
	                		out.println(this.nomMachine);
                		}
	                	System.out.println("[Master]Machine " + nomMachine + " disponible (output: "+ ligne + ")");
                	}
	            	else
	            	{
	            		System.out.println("[Master]Machine " + nomMachine + " indisponible");
	            	}
                }
                else
                {
                	System.out.println("[Master] Sortie Machine brute " + nomMachine + ": " + ligne);
                	// On va détecter les retours des slaves contenant la liste des mots uniques qu'ils ont traités.
                	SlaveCallBackMasterAfterMapping  = (ligne.contains("[UNIQUEWORDS4MASTER_END]")) ? false : SlaveCallBackMasterAfterMapping;
                	SlaveCallBackMasterAfterReducing = (ligne.contains("[REDUCEDWORDS4MASTER_END]")) ? false : SlaveCallBackMasterAfterReducing;
                	if(SlaveCallBackMasterAfterMapping)
                	{ 	// On récupère les mots issus du mapping du slave
                		String[] wordsTreatedBySlave = ligne.trim().split("\\s+");
                		System.out.println("[Master]" + nomMachine + ": UMX_dict > Mots à ajouter: " + wordsTreatedBySlave.length);
                		for(String wordTreatedBySlave: wordsTreatedBySlave)
                		{
                			this.UMX_dict.addUMxFile(wordTreatedBySlave, this.indexFileToTreat);
                		}
                	}
                	if(SlaveCallBackMasterAfterReducing)
                	{
                		// on recup les RMx depuis la sortie standard ex: "Car 3"
                		String[] wordReduceBySlave = ligne.trim().split("\\s+");
                		System.out.println("[Master]" + nomMachine + ": Reduce > Mots traité: " + wordReduceBySlave[0] + "[" + wordReduceBySlave[1] + "]");
                		this.UMX_dict.addWordCount(wordReduceBySlave[0], wordReduceBySlave[1]); //On ajoute le total au dictionnaire
    
                		try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(this.outputPath, true)))) 
	                	{// On ajoute le nom de la machine dans le fichier listeDesMachinesSucces
	                		out.println(this.nomMachine);
                		}
                	}
                	SlaveCallBackMasterAfterMapping  = (ligne.contains("[UNIQUEWORDS4MASTER_BEGIN]")) ? true : SlaveCallBackMasterAfterMapping;
                	SlaveCallBackMasterAfterReducing = (ligne.contains("[REDUCEDWORDS4MASTER_BEGIN]")) ? true : SlaveCallBackMasterAfterReducing;
                	
                }
            }
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
    }
}