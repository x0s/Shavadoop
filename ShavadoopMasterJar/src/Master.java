import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * Représente le master dans l'implémentation d'un Hadoop personnalisé.
 * Le Job initié est un WordCount.
 * 
 * @author x0s
 */
public class Master {

	public static void main(String[] args) 
	{
		String inputFileSrcPath = (args.length == 1) ? args[0] : "src/input.txt"; // "src/large_doc.txt"; input.txt
		
		Job wordCounter = new Job("Word counter");
		wordCounter.launchJob(inputFileSrcPath);
		
		UMx_dict UMX_dict = wordCounter.getUMX_dict();
		System.out.println("----------------------------------------");
		System.out.println(UMX_dict.getWords_UMx().size());
		
		System.out.println(printHashMapStringArrayList(UMX_dict.getWords_UMx()));
	}
	
	
	/**
	 * Permet d'afficher à l'écran le contenu d'un HashMap<String, ArrayList<String>>
	 * 
	 * @param Map	HashMap<String, ArrayList<String>>	HashMap à afficher
	 * @return		String								Résultat affichable
	 */
	public static String printHashMapStringArrayList(HashMap<String, ArrayList<String>> Map) {

	    HashMap<String, ArrayList<String>> map = Map;  
	    String s="\n\t\t Tables: ";
	    Iterator<Entry<String, ArrayList<String>>> iter = map.entrySet().iterator();

	    s= s + " { ";
	    while (iter.hasNext()) {
	        Entry<String, ArrayList<String>> entry = iter.next();

	        List<String> l = new ArrayList<String>();
	        
	        String clef = entry.getKey();
	        l = entry.getValue();
	        String temp="\n[" + clef + " ]";
	        for (int i=1; i<=l.size(); i++){

	            temp= temp + " > " + l.get(i-1);
	        }               
	        s = s + temp;

	        if (iter.hasNext()) {
	            s=s+",";
	        }
	        else s=s+" }";
	    }
	    return s;

	}
}


