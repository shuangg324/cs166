/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

import java.lang.Math;
import java.lang.StringBuilder;
import java.util.Scanner;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class Ticketmaster{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public Ticketmaster(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + Ticketmaster.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		Ticketmaster esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new Ticketmaster (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add User");
				System.out.println("2. Add Booking");
				System.out.println("3. Add Movie Showing for an Existing Theater");
				System.out.println("4. Cancel Pending Bookings");
				System.out.println("5. Change Seats Reserved for a Booking");
				System.out.println("6. Remove a Payment");
				System.out.println("7. Clear Cancelled Bookings");
				System.out.println("8. Remove Shows on a Given Date");
				System.out.println("9. List all Theaters in a Cinema Playing a Given Show");
				System.out.println("10. List all Shows that Start at a Given Time and Date");
				System.out.println("11. List Movie Titles Containing \"love\" Released After 2010");
				System.out.println("12. List the First Name, Last Name, and Email of Users with a Pending Booking");
				System.out.println("13. List the Title, Duration, Date, and Time of Shows Playing a Given Movie at a Given Cinema During a Date Range");
				System.out.println("14. List the Movie Title, Show Date & Start Time, Theater Name, and Cinema Seat Number for all Bookings of a Given User");
				System.out.println("15. EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddUser(esql); break;
					case 2: AddBooking(esql); break;
					case 3: AddMovieShowingToTheater(esql); break;
					case 4: CancelPendingBookings(esql); break;
					case 5: ChangeSeatsForBooking(esql); break;
					case 6: RemovePayment(esql); break;
					case 7: ClearCancelledBookings(esql); break;
					case 8: RemoveShowsOnDate(esql); break;
					case 9: ListTheatersPlayingShow(esql); break;
					case 10: ListShowsStartingOnTimeAndDate(esql); break;
					case 11: ListMovieTitlesContainingLoveReleasedAfter2010(esql); break;
					case 12: ListUsersWithPendingBooking(esql); break;
					case 13: ListMovieAndShowInfoAtCinemaInDateRange(esql); break;
					case 14: ListBookingInfoForUser(esql); break;
					case 15: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice
	
	public static void AddUser(Ticketmaster esql){//1
		String abc = "1234567890abcdef";
		String fname;
		String lname;
		String email;
		String phone;
		String pw;
		try {
			System.out.println("First Name: ");
			fname = in.readLine();
			System.out.println("Last Name: ");
			lname = in.readLine();
			System.out.println("E-mail: ");
			email = in.readLine();
			System.out.println("Phone: ");
			phone = in.readLine();
			System.out.println("Password: ");
			pw = in.readLine();
		} catch (Exception e) {
			System.err.println(e.toString());
			return;
		}
		boolean valid = true;
		int phoneInt = 0;
		if (fname.isEmpty()) {
			valid = false;
			System.out.println("Error: First name cannot be empty.");
		}
		if (lname.isEmpty()) {
			valid = false;
			System.out.println("Error: Last name cannot be empty.");
		}
		if (email.isEmpty()) {
			valid = false;
			System.out.println("Error: Invalid E-Mail.");
		}

		if (!phone.isEmpty()) { // Phone can be empty.
 			try {
				phoneInt = Integer.parseInt(phone);
			} catch (Exception e) {
				valid = false;
				System.out.println("Error: Invalid phone.");
			}
			if (phone.length() != 10) {
				valid = false;
				System.out.println("Error: Phone number must be 10 digits.");
   		}
		}
		if (pw.isEmpty()) {
			valid = false;
			System.out.println("Error: Password cannot be empty.");
		}
		StringBuilder pwbuilder = new StringBuilder(64);
		for (int i = 0; i < 64; i++) {
			pwbuilder.append(abc.charAt((int)(Math.random() * 16.0)));
		}
		if (valid) {
			String query = String.format("INSERT INTO users(fname, lname, email, phone, pwd) VALUES ('%s','%s','%s',%d,'%s');", fname, lname, email, phoneInt, pwbuilder.toString());
			try {
				esql.executeUpdate(query);
			} catch (Exception e) {
				valid = false;
				System.err.println(e.toString());
			}
		} else {
			System.out.println("Query failed; no data affected.");
		}
	}
	
	public static void AddBooking(Ticketmaster esql) throws Exception {//2
		System.out.println("E-Mail: ");
		String email = in.readLine();
		int bid = 0;
		try {
			bid = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT MAX(bid)+1 a FROM bookings;").get(0).get(0));
		} catch (Exception e) {
			System.err.println(e.toString());
			return;
		}
		System.out.println("Status: ");
		String status = in.readLine();
		System.out.println("Seats (space separated): ");
		String seats_String = in.readLine();
		System.out.println("Show ID: ");
		int sid = Integer.parseInt(in.readLine());
		boolean valid = true;
		int itemExists = 0;
		try { 
			itemExists = esql.executeQuery(String.format("SELECT email FROM users WHERE email='%s';", email));
		} catch (Exception e) {
			System.err.println(e.toString());
			return;
		}
		if (itemExists == 0) {
			valid = false;
			System.out.println("Email does not exist in database.");
		}
		itemExists = esql.executeQuery(String.format("SELECT sid FROM shows WHERE sid=%d;", sid));
		if (itemExists == 0) {
			valid = false;
			System.out.println("Show does not exist in database.");
		}
		List<Integer> seats_list = new ArrayList();
		Scanner s = new Scanner(seats_String);
		int seats_num = 0;
		if (valid) {
			while (s.hasNextInt()) {
				seats_num++;
				seats_list.add(s.nextInt());
			}
			if (seats_num == 0) {
				valid = false;
				System.out.println("Error: Specify at least one seat.");
			}
		}
		if (valid) {
			try {
				for (int seat : seats_list) {
					List<List<String>> result_list = esql.executeQueryAndReturnResult(String.format("SELECT bid FROM showseats WHERE sid=%d AND ssid=%d;", sid, seat));
					if (result_list.size() == 0) {
						valid = false;
						System.out.println(String.format("Error: Seat %d doesn't exist for this show", seat));
					} else if (result_list.get(0).get(0) != null) {
						valid = false;
						System.out.println(String.format("Error: Seat %d already booked", seat));
					}
				}
			} catch (Exception e) {
				System.err.println(e.toString());
				valid = false;
			}
		}

		if (valid) {
			try {
				esql.executeUpdate(String.format("INSERT INTO bookings(bid, status, bdatetime, seats, sid, email) VALUES(%d, '%s', NOW(), %d, %d, '%s');", bid, status, seats_num, sid, email));
				for (int seat : seats_list) {
					esql.executeUpdate(String.format("UPDATE showseats SET bid=%d WHERE sid=%d AND ssid=%d;", bid, sid, seat));
				}
			} catch (Exception e) {
				System.err.println(e.toString());
			}
		} else {
			System.out.println("Query failed; no data affected.");
		}
		s.close();
	}
	
	public static void AddMovieShowingToTheater(Ticketmaster esql) throws Exception {//3
		int mvid = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT MAX(mvid)+1 a FROM movies;").get(0).get(0));
		int sid = Integer.parseInt(esql.executeQueryAndReturnResult("SELECT MAX(sid)+1 a FROM shows;").get(0).get(0));
		System.out.println("Movie title: ");
		String title = in.readLine();
		System.out.println("Release date (YYYY-MM-DD):");
		String date = in.readLine();
		System.out.println("Release country:");
		String country = in.readLine();
		System.out.println("Description: ");
		String description = in.readLine();
		System.out.println("Duration (minutes):");
		int duration = Integer.parseInt(in.readLine());
		System.out.println("Language:");
		String lang = in.readLine();
		System.out.println("Genre:");
		String genre = in.readLine();

		System.out.println("Show date (YYYY-MM-DD):");
		String showdate = in.readLine();
		System.out.println("Show start time (HH:MM:SS):");
		String showstime = in.readLine();
		System.out.println("Show end time (HH:MM:SS):");
		String showetime = in.readLine();
		System.out.println("Theater ID: ");
		int tid = Integer.parseInt(in.readLine());

		boolean valid = true;
		int itemExists = esql.executeQuery(String.format("SELECT tid FROM theaters WHERE tid=%d", tid));
		if (itemExists == 0) {
			valid = false;
			System.out.println("Error: Theater does not exist in database.");
		}

		if (valid) {
			try {
				esql.executeUpdate(String.format("INSERT INTO movies(mvid, title, rdate, country, description, duration, lang, genre) VALUES (%d, '%s', to_date('%s', 'YYYY-MM-DD'), '%s', '%s', %d, '%s', '%s');", mvid, title, date, country, description, duration, lang, genre));
				esql.executeUpdate(String.format("INSERT INTO shows(sid, mvid, sdate, sttime, edtime) VALUES(%d, %d, to_date('%s', 'YYYY-MM-DD'), to_timestamp('%s', 'HH24:MM:SS'), to_timestamp('%s', 'HH24:MM:SS'));", sid, mvid, showdate, showstime, showetime));
				esql.executeUpdate(String.format("INSERT INTO plays(sid, tid) VALUES(%d, %d);", sid, tid));
			} catch (Exception e) {
				System.out.println(e.toString());
			}
		}
	}
	
	public static void CancelPendingBookings(Ticketmaster esql) throws IOException {//4
		try {
			esql.executeUpdate("UPDATE showseats SET bid=NULL WHERE bid IN (SELECT b.bid FROM showseats s INNER JOIN bookings b ON b.bid=s.bid WHERE b.status='Pending');");
			esql.executeUpdate("UPDATE bookings SET status='Cancelled' WHERE status='Pending';");
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}
	
	public static void ChangeSeatsForBooking(Ticketmaster esql) throws Exception{//5
		System.out.println("Booking ID:");
		boolean valid = true;
		int bid = Integer.parseInt(in.readLine());
		int sid = 0;
		try {
			sid = Integer.parseInt(esql.executeQueryAndReturnResult(String.format("SELECT sid FROM bookings WHERE bid=%d", bid)).get(0).get(0));
		} catch (Exception e) {
			System.err.println(e.toString());
			valid = false;
		}

		List<List<String>> results = new ArrayList<>();
		int price = 0;
		int newprice = 0;

		try {
			results = esql.executeQueryAndReturnResult(String.format("SELECT ssid, price FROM showseats WHERE bid=%d;", bid));
		} catch (Exception e) {
			System.out.println(e.toString());
			valid = false;
		}
		List<List<String>> result_list = new ArrayList<>();
		List<Integer> seats_list = new ArrayList<>();
		if (valid) {
			price = 0;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < results.size(); i++) {
				price += Integer.parseInt(results.get(i).get(1));
				sb.append(results.get(i).get(0));
				if (i < results.size() - 1) {
					sb.append(", ");
				}
			}
			System.out.println(String.format("You currently have seat(s) %s booked. New seats: ", sb.toString()));
			String newseats = in.readLine();
			Scanner s = new Scanner(newseats);
			int seats_num = 0;
			while (s.hasNextInt()) {
				seats_num++;
				int seat = s.nextInt();
				seats_list.add(seat);
			}
			s.close();
			if (seats_num == 0) {
				valid = false;
				System.out.println("Error: Specify at least one seat.");
			}

			for (int seat : seats_list) {
				result_list = esql.executeQueryAndReturnResult(String.format("SELECT bid, price FROM showseats WHERE ssid=%d", seat));
				if (result_list.size() == 0) {
					valid = false;
					System.out.println(String.format("Error: Seat %d doesn't exist for this show", seat));
				} else if (result_list.get(0).get(0) != null) {
					valid = false;
					System.out.println(String.format("Error: Seat %d already booked", seat));
				}
				newprice += Integer.parseInt(result_list.get(0).get(1));
			}
		}

		if (newprice != price) {
			valid = false; System.out.println(String.format("Error: Prices don't match! Old price: %d; new price: %d", price, newprice));
		}
		if (valid) {
			try {
				esql.executeUpdate(String.format("UPDATE bookings SET seats=%d WHERE bid=%d;", result_list.size(), bid));
				esql.executeUpdate(String.format("UPDATE showseats SET bid=NULL WHERE bid=%d;", bid));
				for (int seat : seats_list) {
					esql.executeUpdate(String.format("UPDATE showseats SET bid=%d WHERE sid=%d AND ssid=%d;", bid, sid, seat));
				}
			} catch (Exception e) {
				System.out.println(e.toString());
			}
		}
	}
	
	public static void RemovePayment(Ticketmaster esql) throws IOException {//6
        //
	}
	
	public static void ClearCancelledBookings(Ticketmaster esql) throws IOException {//7
		try {
			esql.executeUpdate("DELETE FROM bookings WHERE status='Cancelled';");
		} catch (Exception e) {
			System.err.println(e.toString());
		}
	}
	
	public static void RemoveShowsOnDate(Ticketmaster esql){//8
        //
	}
	
	public static void ListTheatersPlayingShow(Ticketmaster esql) {//9 
	// List all Theaters in a Cinema Playing a Given Show
	    List<List<String>> theaters = new ArrayList<>();
	    
	    System.out.println("Enter show: ");
	    String show = in.readLine(); //user inputted show
	    
		try {
		     theaters = esql.executeQueryAndReturnResult(String.format("SELECT theater FROM Cinemas WHERE show = %d", show));
		}
		catch  (Exception e) {
				System.err.println(e.toString());
			}
		
		
	}
	
	public static void ListShowsStartingOnTimeAndDate(Ticketmaster esql){//10
		// List all Shows that Start at a Given Time and Date
		List<List<String>> shows = new ArrayList<>();
		
		System.out.println("Enter show starting time: ");
		int time = Integer.parseInt(in.readLine()); //user inputs time
		System.out.println("Enter date of show: ");
		int date = Integer.parseInt(in.readLine()); //user inputs date
		
		try {
		    shows = esql.executeQueryAndReturnResult(String.format("SELECT sid FROM Shows WHERE time = %d, date = %d", time, date));
		}
		
		catch  (Exception e) {
				System.err.println(e.toString());
			}
		
	}

	public static void ListMovieTitlesContainingLoveReleasedAfter2010(Ticketmaster esql){//11
		//List Movie Titles Containing “love” Released After 2010
		
		try {
		    String movies = "SELECT title FROM Movies WHERE title LIKE %love% AND release_date > '2010-12-31' ";
		    
		    System.out.println("\n\n -- EXECUTING QUERY -- \n\n");
		    esql.executeQueryAndReturnResult(movies);
		    System.out.println("\n\n -- QUERY RESULTS -- \n\n");
		}
		
		catch  (Exception e) {
				System.err.println(e.toString());
			}
	}

	public static void ListUsersWithPendingBooking(Ticketmaster esql){//12
		//List the First Name, Last Name, and Email of Users with a Pending Booking
		
		try {
		    String users = ("SELECT fname, lname, email FROM Users WHERE status = (SELECT status FROM Bookings WHERE status = 'Pending' )");
		    
		    
		    System.out.println("\n\n -- EXECUTING QUERY -- \n\n");
		    esql.executeQueryAndReturnResult(users);
		    System.out.println("\n\n -- QUERY RESULTS -- \n\n");
		}
		
		catch  (Exception e) {
				System.err.println(e.toString());
			}
		
	}

	public static void ListMovieAndShowInfoAtCinemaInDateRange(Ticketmaster esql){//13
		//List the Title, Duration, Date, and Time of Shows Playing a Given Movie at a Given Cinema During a Date Range
		String date;
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("YYYY-MM-DD");
		
		while(true) {
		    System.out.println("Enter movie date (YYYY-MM-DD): ");
		    try {
		        date = in.readLine();
		        LocalDate movie_date = LocalDate.parse(date, formatter);
		        break;
		    }
		    catch (Exception e) {
		        System.out.println("Invalid input. Try again");
		        continue;
		    }
		    
		}
		
		List<List<String>> info = new ArrayList<>();
		
		String movie = in.readLine();
		String cinema = in.readLine();
		
		
		try {
		    info = esql.executeQueryAndReturnResult(String.format("SELECT title, duration, date, time FROM Shows, Movies WHERE Movie = %d, Cinema = %d, date  ", movie, cinema ));
		}
		
		catch  (Exception e) {
				System.err.println(e.toString());
			}
		
	}    
		
		
	public static void ListBookingInfoForUser(Ticketmaster esql){//14
		//List the Movie Title, Show Date & Start Time, Theater Name, and Cinema Seat Number for all Bookings of a Given User
		
		System.out.println("Enter email: ");
		
		String user = in.readLine();
		
		List<List<String>> bookings = new ArrayList<>();
		
		try{
		     bookings = esql.executeQueryAndReturnResult(String.format("SELECT title, date, stime, tname, snum FROM Movies, Shows, Theaters, CinemaSeats, Bookings WHERE user = %d", user));
		}
		catch  (Exception e) {
				System.err.println(e.toString());
			}
	}
	
}