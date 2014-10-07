import java.util.*;
import java.io.*;
public class AverageTempNoHadoop
{
  public static void main(String[]args)throws IOException
  {
    File file = new File("MRTest-1.txt");
    Scanner input = new Scanner(file);
    HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
    HashMap<Integer,Integer> totalMap = new HashMap<Integer,Integer>();
    while(input.hasNext())
    {
      String tokens[] = input.nextLine().split(" ");
      int key = Integer.parseInt(tokens[0]);
      int value = Integer.parseInt(tokens[1]);

      if(map.containsKey(key))
      {
	map.put(key,map.get(key)+value);
	totalMap.put(key,totalMap.get(key)+1);
      }
      else
      {
	map.put(key,value);
	totalMap.put(key,1);
      }

    }
    for(Integer k: map.keySet())
    {
      System.out.println(k+" "+map.get(k)/totalMap.get(k));
    }
  }
}
