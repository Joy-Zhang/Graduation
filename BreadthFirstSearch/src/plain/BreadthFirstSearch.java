package plain;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import common.*;

public class BreadthFirstSearch {
	public static void main(String[] args) throws IOException {
		GraphIndexer indexer = new GraphIndexer();
		indexer.init();
		
		Path start = new Path("/tmp/start");
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(start, true);
		System.out.println("!!!!");

		fs.mkdirs(output);
		FileUtil.copy(fs, new Path(args[0]), fs, start, false, new Configuration());
		int i = 0;
		do {
			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(start)));
			
			String line = reader.readLine();
			HashSet<Integer> neibourSet = new HashSet<Integer>(); 
			OutputStream out = fs.create(
					new Path(args[1] + Path.SEPARATOR + String.valueOf(i)));
			while(line != null) {
				int vertex = Integer.parseInt(line.trim());
				indexer.markupAccessed(vertex);
				int[] neighbours = indexer.queryNeighbours(vertex);
				for(int neighbour : neighbours)
				{
					if(!indexer.isAccessed(neighbour))
						neibourSet.add(new Integer(neighbour));
				}
				//System.out.print(line+" ");
				line = reader.readLine();
				out.write(String.format("%d\n", vertex).getBytes());
				
			}
			//System.out.println();
			reader.close();
			out.flush();
			out.close();
			fs.delete(start, true);
			OutputStream result = fs.create(start);
			for(Integer vertex : neibourSet) {
				String neighbour = String.format("%d\n", vertex.intValue());
				//System.out.print("   " + neighbour);
				result.write(neighbour.getBytes());
				
			}
			result.flush();
			result.close();
			i++;
		} while(!indexer.isFinished());
		indexer.close();
	}
}
