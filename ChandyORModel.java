import java.io.*;  
import java.net.*;
import java.util.*;
import java.math.*;
import java.text.*;
import java.util.concurrent.ThreadLocalRandom;

public class ChandyORModel {
	public enum msgType {
		QUERY, REPLY
	}

	public enum processType {
		ACTIVE, WAITING, DEADLOCKED
	}

	static Integer myId = 0;
	static Integer numProcess = 0;
	static ArrayList<String> ipList = new ArrayList<String>();
	// adjList stores adjacency list corresponding to network topology
	static ArrayList<ArrayList<Integer>> adjList = new ArrayList<ArrayList<Integer>>();
	// wfgList stores adjacency list corresponding to the wait-for-graph
	static ArrayList<ArrayList<Integer>> wfgList = new ArrayList<ArrayList<Integer>>();
	// routing table corresponding to each process
	static ArrayList<ArrayList<Integer>> routing = new ArrayList<ArrayList<Integer>>();
	// socket connections info
	static HashMap<Integer, Socket> socketMap = new HashMap<Integer, Socket>();
	static processType myType = processType.ACTIVE;
	static Boolean probeFlag = false;
	/* 
		wait(i) is true if I was continuously blocked 
		since I received the first deadlock probe(engaging query) initiated by i
	*/
	static ArrayList<Boolean> wait = new ArrayList<Boolean>();
	/* 
		num(i) is number of active probes corresponding to deadlock probe of i. 
		If num(myId) == 0, then myId has detected deadlock 
	*/
	static ArrayList<Integer> num = new ArrayList<Integer>();
	/* 
		engager(i) is the process who sent me the first deadlock probe of i
	*/ 
	static HashMap<Integer, Integer> engager = new HashMap<Integer, Integer>();
	
	/*
	* getHash(x, y) generates a unique number for every unordered pair (x, y)
	* @Params: 
	* 	Integer x: Unique id1
	* 	Integer y: Unique id2
	* @Returns: 
	* 	Integer hashVal: Unique hash value corresponding to pair (x, y). 
	*			 This helps in establishing socket connections later.
	*/
	static Integer getHash(Integer x, Integer y) {
		Integer hashVal = numProcess * Math.min(x, y) + Math.max(x, y) + 2000;
		return hashVal;
	}
	
	/*
	* bfs(curr) produces the routing table for process <curr>
	* @Params
	* 	Integer curr: id of the node for which we are running BFS.
	* @Returns
	* 	Boolean reachable: true if all the nodes in the graph are reachable from curr, false otherwise.
	*/	

	public static Boolean bfs(Integer curr) {
		Integer cntVisited = 0;
		ArrayList<Boolean> visited = new ArrayList<Boolean>();
		for(int i = 0; i < numProcess; i++) {
			visited.add(false);
		}
		routing.get(curr).set(curr, -1);
		visited.set(curr, true);
		cntVisited++;
		Queue<Integer> nodes = new LinkedList<Integer>();
		nodes.add(-1);
		nodes.add(curr);
		while(!nodes.isEmpty()) {
			Integer par = nodes.poll();
			Integer x = nodes.poll();
			for(Integer neigh: adjList.get(x)) {
				if(!visited.get(neigh)) {
					if(par == -1) {
						nodes.add(curr);
						nodes.add(neigh);
						routing.get(curr).set(neigh, neigh);
					} else if(par == curr) {
						nodes.add(x);
						nodes.add(neigh);
						routing.get(curr).set(neigh, x);
					} else {
						nodes.add(par);
						nodes.add(neigh);
						routing.get(curr).set(neigh, par);
					}
					visited.set(neigh, true);
					cntVisited++;
				}
			}
		}
		Boolean reachable = (cntVisited == numProcess);
		return reachable;
	}
	
	/*
	* makeMsg(orig, sender, target, flag) produces a message string
	* @Params:
	*	Integer orig: Originator of the deadlock query
	*	Integer sender: current source
	*	Integer target: current destination
	*	Integer flag: message type - either QUERY or REPLY
	* @Returns:
	* 	String msg: concatenate all the params to create a message string
	*/
	static String makeMsg(Integer orig, Integer sender, Integer target, Integer flag) {
		String msg = orig.toString() + " " + sender.toString() + " " + target.toString() + " " + flag.toString();
		return msg;
	}
	
	/*
	* doWork(routing) is the worker function. 
	* @Params:
	* 	routing: ArrayList corresponding to the neighbors of current process.
	*/
	static void doWork(ArrayList<Integer> routing) throws Exception {
		// if the probe flag is true, process starts the deadlock detection
		if (probeFlag) {
		
			// book-keeping
			wait.set(myId, true);
			num.set(myId, wfgList.get(myId).size());
			engager.put(myId, myId);
			
			// now forward the query to all my dependents
			for(Integer x: wfgList.get(myId)) {
				Integer intermediate = routing.get(x);
				String probeMsg = makeMsg(myId, myId, x, msgType.QUERY.ordinal());
				System.out.println((myId + 1) + " sent a QUERY message to " + (x + 1)); 
				DataOutputStream dout = new DataOutputStream(socketMap.get(intermediate).getOutputStream());
				dout.writeUTF(probeMsg);
				dout.flush();
			}
		}
		// Listen infinitely on all my connections 
		while(true) {
			for(Integer x: adjList.get(myId)) {
				String msg;
				try {
					DataInputStream dis = new DataInputStream(socketMap.get(x).getInputStream());
					msg = (String)dis.readUTF();
				} catch(Exception e) {	//No message
					continue;
				}
				
				// parse the message
				String[] inMsg = msg.split(" ");
				Integer orig = Integer.parseInt(inMsg[0]);
				Integer sender = Integer.parseInt(inMsg[1]);
				Integer target = Integer.parseInt(inMsg[2]);
				msgType type = msgType.values()[Integer.parseInt(inMsg[3])];
				
				if (target != myId) {	// I'm not the target, just route it forward
					String queryMsg = makeMsg(orig, sender, target, type.ordinal());
					DataOutputStream dout = new DataOutputStream(socketMap.get(routing.get(target)).getOutputStream());
					dout.writeUTF(queryMsg);
					dout.flush();
					continue;
				}
				if (type == msgType.QUERY) {	// It's for me and it is a QUERY message
					if (myType == processType.WAITING) {	// If I am blocked process the query, if I am active discard it.
						if(engager.get(orig) == null) {	// if it is the first probe by orig
						
							System.out.println((myId + 1) + " received a ENGAGING QUERY from " + (sender + 1));
							
							// more book-keeping
							engager.put(orig, sender);
							num.set(orig, wfgList.get(myId).size());
							wait.set(orig, true);
							
							// Forward the query to my dependents
							for(Integer wf: wfgList.get(myId)) {
								Integer intermediate = routing.get(wf);
								String queryMsg = makeMsg(orig, myId, wf, msgType.QUERY.ordinal());
								DataOutputStream dout = new DataOutputStream(socketMap.get(intermediate).getOutputStream());
								dout.writeUTF(queryMsg);
								System.out.println((myId + 1) + " sent a QUERY message to " + (wf + 1));
								dout.flush();
							}
						} else if (wait.get(orig)) { // if it is a repeat query and I've been blocked since orig's engaging query
							System.out.println((myId + 1) + " received a REPEAT QUERY from " + (sender + 1));
							// Send a reply message to the sender
							Integer intermediate = routing.get(sender);
							String replyMsg = makeMsg(orig, myId, sender, msgType.REPLY.ordinal());
							DataOutputStream dout = new DataOutputStream(socketMap.get(intermediate).getOutputStream());
							dout.writeUTF(replyMsg);
							dout.flush();
							System.out.println((myId + 1) + " sent a REPLY message to " + (sender + 1));
						}
					}
				} else { // It's for me and it is a REPLY message
					if (wait.get(orig)) { // if I've been blocked since orig's engaging query, otherwise discard
						System.out.println((myId + 1) + " received a REPLY message from " + (sender + 1));
						
						// REPLY message means the channel from sender is blocked, hence decrement num(orig)
						Integer temp = num.get(orig); 
						num.set(orig, temp - 1);
						
						if (num.get(orig) == 0) { // if all channels from myId are blocked
							if (orig == myId) {	// and I initiated the probe query, then deadlock detected
								System.out.println("Deadlock detected");
								myType = processType.DEADLOCKED;
							} else {
								// Send engager(orig) a reply message suggesting all channels from me are blocked
								Integer en = engager.get(orig);
								Integer intermediate = routing.get(en);
								String replyMsg = makeMsg(orig, myId, en, msgType.REPLY.ordinal());
								DataOutputStream dout = new DataOutputStream(socketMap.get(intermediate).getOutputStream());
								dout.writeUTF(replyMsg);
								System.out.println((myId + 1) + " sent a REPLY message to " + (en + 1));
								dout.flush();
							}
						}
					}
				}
			}
		}
	}
	
	/*
	* getList(isDirected, inReader): A generic function to populate graphs given topology
	* 	For e.g: 1 2 3 translates to adjList[0] containing items 1 and 2 (zero-indexing ;)
	* @Params:
	* 	Boolean isDirected: true if the graph is directed
	*	BufferedReader: character input stream to read from
	* @Returns:
	* 	ArrayList<ArrayList<Integer>>: adjList corresponding to the topology extracted from BufferedReader
	*/
	static ArrayList<ArrayList<Integer>> getList(Boolean isDirected, BufferedReader inReader) throws IOException {
		ArrayList<ArrayList<Integer>> adjList = new ArrayList<ArrayList<Integer>>();
		while(adjList.size() < numProcess) {
			adjList.add(new ArrayList<Integer>());
		}
		for (Integer i = 0; i < numProcess; i++) {
			String tempLine;
			tempLine = inReader.readLine();
			String[] nums = tempLine.split(" ");
			Integer curr = Integer.parseInt(nums[0]);
			curr--;
			for(Integer j = 1; j < nums.length; j++) {
				Integer x = Integer.parseInt(nums[j]);
				x--;
				adjList.get(curr).add(x);
				if(!isDirected) {
					adjList.get(x).add(curr);
				}
			}
		}
		return adjList;
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Usage: java ChandyORModel <id> <probe flag>");
			System.exit(-1);
		}

		myId = Integer.parseInt(args[0]);
		myId--;
		
		if(Integer.parseInt(args[1]) == 1) {
			probeFlag = true;
		}
		
		BufferedReader inReader = new BufferedReader(new FileReader("inp-params.txt"));
		String tempLine = inReader.readLine();
		String[] nums = tempLine.split(" ");
		numProcess = Integer.parseInt(nums[0]);
		
		// ipList is the mapping between node ids and their IP address
		for (Integer i = 0; i < numProcess; i++) {
			tempLine = inReader.readLine();
			nums = tempLine.split(" ");
			ipList.add(nums[2]);
		}
		 
		// Initialisation
		while(adjList.size() < numProcess) {
			adjList.add(new ArrayList<Integer>());
			wfgList.add(new ArrayList<Integer>());
			routing.add(new ArrayList<Integer>());
			wait.add(false);
			num.add(0);
		}
		
		// Call getList function to populate adjList and wfgList
		adjList = getList(false, inReader);
		wfgList = getList(true, inReader);
	
		//Making adjacency lists unique and initialise routing table
		for(int i = 0; i < numProcess; i++) {
			// add contents of adjList(i) to a temporary HashSet 
			HashSet<Integer> temp = new HashSet<>();
			temp.addAll(adjList.get(i));
			// clear adjList(i)
			adjList.get(i).clear();
			// add temp hashset to adjList(i). Now all the elements are unique. Sort the adjList now. Yayy! 
			adjList.get(i).addAll(temp);
			Collections.sort(adjList.get(i));
		
			// Same stuff for wfgList
			temp = new HashSet<>();
			temp.addAll(wfgList.get(i));
			wfgList.get(i).clear();
			wfgList.get(i).addAll(temp);
			// initialise the routing table for ith process to all zeroes
			for(int j = 0; j < numProcess; j++) {
				routing.get(i).add(0);
			}
		}

		// Populate the actual routes corresponding to each process. 
		// Caution: the graph should be connected
		for(Integer i = 0; i < numProcess; i++) {
			if(!bfs(i)) {
				System.out.println("Topology not connected");
				System.exit(-1);
			}
		}
		
		// Establising socket connections
		for(Integer neigh: adjList.get(myId)) {	
			if(neigh < myId) { // if neighbour's id is smaller, I am the client
				while(true) {
					try {
						Socket s = new Socket(ipList.get(neigh), getHash(neigh, myId));
						s.setSoTimeout(1000);
						socketMap.put(neigh, s);
						break;
					} catch (ConnectException e) {
						continue;
					}
				}
			} else { // otherwise I'm the server and accept connections
				ServerSocket ss = new ServerSocket(getHash(neigh, myId));
				Socket s = ss.accept();
				s.setSoTimeout(1000);
				socketMap.put(neigh, s);
			}
		}
		
		// if my outdegree in the wait-for-graph is greater than 1, then I'm waiting on some resources
		if (wfgList.get(myId).size() > 0) {
			myType = processType.WAITING;
		}

		doWork(routing.get(myId));
	}
}
