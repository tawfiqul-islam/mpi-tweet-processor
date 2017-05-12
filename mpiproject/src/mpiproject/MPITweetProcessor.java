package mpiproject;
import java.io.*;
import java.util.*;
import org.json.*;
import mpi.*;
/**
 * COMP90024 Assignment-1
 * @author Muhammed Tawfiqul Islam
 * Student ID: 797438
 * Cloud Computing and Distributed Systems (CLOUDS) Laboratory
 * The University of Melbourne
 * Date: 24-03-2017
 */
class Grid implements Comparable<Grid>{
	
	double xmin,xmax,ymin,ymax;
	String id;
	int tweetCounts=0;
	
	boolean inGrid(double x, double y)
	{
		if(x>=xmin&&x<=xmax&&y>=ymin&&y<=ymax)
		{
			tweetCounts++;
			return true;
		}
		else
			return false;
	}
	
	public int compareTo(Grid o) {
		return (int)(o.tweetCounts - this.tweetCounts);
	}
}

public class MPITweetProcessor {

	static public void main(String[] args) throws MPIException {
		

		long fileSize = 0;
		String appArgs[]= MPI.Init(args);
		System.out.println(appArgs.length);
		for(int i=0;i<appArgs.length;i++)
			System.out.println("i: "+i+" "+appArgs[i]);
		
		MPI.COMM_WORLD.Barrier();
		int myrank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size() ;
		long starttime = 0;
		if(myrank==0)
			starttime=System.currentTimeMillis();
		File file= new File(appArgs[1]);
		fileSize = file.length();
		long myPortion=fileSize/size;
		long myStart=myPortion*myrank;
		if(myrank==size-1) {
			myPortion=fileSize-myStart;
		}
		Grid mygrid[] = new Grid[16];
		parseGridFile(mygrid,appArgs[0]);
		parseInputandFindGrid(myStart,myPortion,mygrid,appArgs[1]);
		MPI.COMM_WORLD.Barrier();
		if(myrank==0) {	
			System.out.println("Master #"+MPI.COMM_WORLD.Rank()+" - Collecting Results from Workers");
			int recvbuf[] = new int[mygrid.length];
			for(int i=1;i<size;i++) {
				MPI.COMM_WORLD.Recv(recvbuf, 0, mygrid.length, MPI.INT, i, 100);
				System.out.println("Master #"+MPI.COMM_WORLD.Rank()+" - Received result from Worker #"+i);

				for(int j=0;j<mygrid.length;j++)
					mygrid[j].tweetCounts+=recvbuf[j];
			}

			System.out.println("***RESULTS***");
			Grid rowGrid[] = new Grid[4];
			Grid columnGrid[]=new Grid[5];
			for(int i=0;i<rowGrid.length;i++)
				rowGrid[i]=new Grid();
			for(int i=0;i<columnGrid.length;i++)
				columnGrid[i]=new Grid();
			
			rowGrid[0].tweetCounts=mygrid[0].tweetCounts+mygrid[1].tweetCounts+mygrid[2].tweetCounts+mygrid[3].tweetCounts;
			rowGrid[0].id="Row 1";
			rowGrid[1].tweetCounts=mygrid[4].tweetCounts+mygrid[5].tweetCounts+mygrid[6].tweetCounts+mygrid[7].tweetCounts;
			rowGrid[1].id="Row 2";
			rowGrid[2].tweetCounts=mygrid[8].tweetCounts+mygrid[9].tweetCounts+mygrid[10].tweetCounts+mygrid[11].tweetCounts+mygrid[12].tweetCounts;
			rowGrid[2].id="Row 3";
			rowGrid[3].tweetCounts=mygrid[13].tweetCounts+mygrid[14].tweetCounts+mygrid[15].tweetCounts;
			rowGrid[3].id="Row 4";
			
			columnGrid[0].tweetCounts=mygrid[0].tweetCounts+mygrid[4].tweetCounts+mygrid[8].tweetCounts;
			columnGrid[0].id="Column 1";
			columnGrid[1].tweetCounts=mygrid[1].tweetCounts+mygrid[5].tweetCounts+mygrid[9].tweetCounts;
			columnGrid[1].id="Column 2";
			columnGrid[2].tweetCounts=mygrid[2].tweetCounts+mygrid[6].tweetCounts+mygrid[10].tweetCounts+mygrid[13].tweetCounts;
			columnGrid[2].id="Column 3";
			columnGrid[3].tweetCounts=mygrid[3].tweetCounts+mygrid[7].tweetCounts+mygrid[11].tweetCounts+mygrid[14].tweetCounts;
			columnGrid[3].id="Column 4";
			columnGrid[4].tweetCounts=mygrid[12].tweetCounts+mygrid[15].tweetCounts;
			columnGrid[4].id="Column 5";
			
			Arrays.sort(mygrid);
			Arrays.sort(rowGrid);
			Arrays.sort(columnGrid);
			
			System.out.println("-> Rank of Grid Boxes:");
			for(int i=0;i<mygrid.length;i++) {
				System.out.println(mygrid[i].id+": "+mygrid[i].tweetCounts+" tweets");
			}
			
			System.out.println("-> Rank of Grid Rows:");
			for(int i=0;i<rowGrid.length;i++) {
				System.out.println(rowGrid[i].id+": "+rowGrid[i].tweetCounts+" tweets");
			}
			
			System.out.println("-> Rank of Grid Columns:");
			for(int i=0;i<columnGrid.length;i++) {
				System.out.println(columnGrid[i].id+": "+columnGrid[i].tweetCounts+" tweets");
			}
			System.out.println("Master #"+MPI.COMM_WORLD.Rank()+" - Finished Processing in: "+(System.currentTimeMillis()-starttime)/1000+" seconds!");
		}
		else {
			System.out.println("Worker #"+MPI.COMM_WORLD.Rank()+" - Sending results to master.");
			int sendbuf[] = new int[mygrid.length];
			for(int i=0;i<mygrid.length;i++) {
				sendbuf[i]=mygrid[i].tweetCounts;
			}
			MPI.COMM_WORLD.Send(sendbuf, 0, mygrid.length, MPI.INT, 0, 100);	
		}
		MPI.Finalize();
	}
	
	public static void parseInputandFindGrid(long start, long myPortion, Grid[] mygrid, String inputFilePath) {

		try {
			FileInputStream in = new FileInputStream(inputFilePath);
			
			try {
				in.skip(start);
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			if(MPI.COMM_WORLD.Rank()!=0) {
				while(true) {
					int p = 0;
					try {
						p = in.read();
					} catch (IOException e) {
						e.printStackTrace();
					}
					myPortion--;
					if((char)p=='\n') {
						break;
					}				
				}
			}
			long count=0;
			long errcount=0;
			Scanner sc = new Scanner(in,"UTF-8");
			while(myPortion>0) {
				
				String str = null;
				if(sc.hasNextLine()) {
					str = sc.nextLine();
					try {
						myPortion=myPortion-(str.getBytes("UTF-8").length+1);
						count++;
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
				}
				else {
					System.out.println("Worker #"+MPI.COMM_WORLD.Rank()+" - End of InputStream!");
					break;
				}
				JSONObject obj = null;
				try {
					obj = new JSONObject(str);
				} catch (JSONException e) {
					errcount++;
					continue;
					//e.printStackTrace();
				}
				try {
					JSONArray coordinates = obj.getJSONObject("json").getJSONObject("coordinates").getJSONArray("coordinates");
					for(int j=0;j<mygrid.length;j++) {
						if(mygrid[j].inGrid(coordinates.getDouble(0), coordinates.getDouble(1)))
							break;
					}

				} catch (JSONException e) {
					//	e.printStackTrace();
				}
			}
			System.out.println("Worker #"+MPI.COMM_WORLD.Rank()+" - Total parsed lines: "+count);
			System.out.println("Worker #"+MPI.COMM_WORLD.Rank()+" - Total errors found: "+errcount);	
			try {
				in.close();
				sc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e2) {
			e2.printStackTrace();
		}
	}
	
	public static void parseGridFile(Grid[] mygrid, String gridfilepath) {
		File melbGridFile = new File(gridfilepath);

		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(melbGridFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String line;
		int gridIndex=0;
		try {
			while ((line = br.readLine()) != null) {

				JSONObject obj = null;
				try {
					obj = new JSONObject(line);
				} catch (JSONException e) {
					continue;
					//e.printStackTrace();
				}
				try {
					mygrid[gridIndex]=new Grid();
					mygrid[gridIndex].id=(String) obj.getJSONObject("properties").get("id");
					mygrid[gridIndex].xmin=(Double) obj.getJSONObject("properties").get("xmin");
					mygrid[gridIndex].xmax=(Double) obj.getJSONObject("properties").get("xmax");
					mygrid[gridIndex].ymin=(Double) obj.getJSONObject("properties").get("ymin");
					mygrid[gridIndex].ymax=(Double) obj.getJSONObject("properties").get("ymax");

					gridIndex++;

				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
