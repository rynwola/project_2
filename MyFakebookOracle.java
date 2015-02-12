package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;
import java.sql.PreparedStatement;

public class MyFakebookOracle extends FakebookOracle {
	
	static String prefix = "ethanjyx.";
	
	// You must use the following variable as the JDBC connection
	Connection oracleConnection = null;
	
	// You must refer to the following variables for the corresponding tables in your database
	String cityTableName = null;
	String userTableName = null;
	String friendsTableName = null;
	String currentCityTableName = null;
	String hometownCityTableName = null;
	String programTableName = null;
	String educationTableName = null;
	String eventTableName = null;
	String participantTableName = null;
	String albumTableName = null;
	String photoTableName = null;
	String coverPhotoTableName = null;
	String tagTableName = null;
	
	// DO NOT modify this constructor
	public MyFakebookOracle(String u, Connection c) {
		super();
		String dataType = u;
		oracleConnection = c;
		// You will use the following tables in your Java code
		cityTableName = prefix+dataType+"_CITIES";
		userTableName = prefix+dataType+"_USERS";
		friendsTableName = prefix+dataType+"_FRIENDS";
		currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
		hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
		programTableName = prefix+dataType+"_PROGRAMS";
		educationTableName = prefix+dataType+"_EDUCATION";
		eventTableName = prefix+dataType+"_USER_EVENTS";
		albumTableName = prefix+dataType+"_ALBUMS";
		photoTableName = prefix+dataType+"_PHOTOS";
		tagTableName = prefix+dataType+"_TAGS";
	}
	
	
	@Override
	// ***** Query 0 *****
	// This query is given to your for free;
	// You can use it as an example to help you write your own code
	//
	public void findMonthOfBirthInfo() throws SQLException{ 
		ResultSet rst = null; 
		PreparedStatement getMonthCountStmt = null;
		PreparedStatement getNamesMostMonthStmt = null;
		PreparedStatement getNamesLeastMonthStmt = null;
		
		try {
			// Scrollable result set allows us to read forward (using next())
			// and also backward.  
			// This is needed here to support the user of isFirst() and isLast() methods,
			// but in many cases you will not need it.
			// To create a "normal" (unscrollable) statement, you would simply call
			// stmt = oracleConnection.prepareStatement(sql);
			//
			String getMonthCountSql = "select count(*), month_of_birth from " +
				userTableName +
				" where month_of_birth is not null group by month_of_birth order by 1 desc";
			getMonthCountStmt = oracleConnection.prepareStatement(getMonthCountSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			
			// getMonthCountSql is the query that will run
			// For each month, find the number of friends born that month
			// Sort them in descending order of count
			// executeQuery will run the query and generate the result set
			rst = getMonthCountStmt.executeQuery();
			
			this.monthOfMostFriend = 0;
			this.monthOfLeastFriend = 0;
			this.totalFriendsWithMonthOfBirth = 0;
			while(rst.next()) {
				int count = rst.getInt(1);
				int month = rst.getInt(2);
				if (rst.isFirst())
					this.monthOfMostFriend = month;
				if (rst.isLast())
					this.monthOfLeastFriend = month;
				this.totalFriendsWithMonthOfBirth += count;
			}
			
			// Get the month with most friends, and the month with least friends.
			// (Notice that this only considers months for which the number of friends is > 0)
			// Also, count how many total friends have listed month of birth (i.e., month_of_birth not null)
			//
			
			// Get the names of friends born in the "most" month
			String getNamesMostMonthSql = "select user_id, first_name, last_name from " + 
				userTableName + 
				" where month_of_birth = ?";
			getNamesMostMonthStmt = oracleConnection.prepareStatement(getNamesMostMonthSql);
			
			// set the first ? in the sql above to value this.monthOfMostFriend, with Integer type
			getNamesMostMonthStmt.setInt(1, this.monthOfMostFriend);
			rst = getNamesMostMonthStmt.executeQuery();
			while(rst.next()) {
				Long uid = rst.getLong(1);
				String firstName = rst.getString(2);
				String lastName = rst.getString(3);
				this.friendsInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
			}
			
			// Get the names of friends born in the "least" month
			String getNamesLeastMonthSql = "select first_name, last_name, user_id from " + 
				userTableName + 
				" where month_of_birth = ?";
			getNamesLeastMonthStmt = oracleConnection.prepareStatement(getNamesLeastMonthSql);
			getNamesLeastMonthStmt.setInt(1, this.monthOfLeastFriend);
			
			rst = getNamesLeastMonthStmt.executeQuery();
			while(rst.next()){
				String firstName = rst.getString(1);
				String lastName = rst.getString(2);
				Long uid = rst.getLong(3);
				this.friendsInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getMonthCountStmt != null)
				getMonthCountStmt.close();
			
			if(getNamesMostMonthStmt != null)
				getNamesMostMonthStmt.close();
			
			if(getNamesLeastMonthStmt != null)
				getNamesLeastMonthStmt.close();
		}
	}

	
	
	@Override
	// ***** Query 1 *****
	// Find information about friend names:
	// (1) The longest last name (if there is a tie, include all in result)
	// (2) The shortest last name (if there is a tie, include all in result)
	// (3) The most common last name, and the number of times it appears (if there is a tie, include all in result)
	//
	public void findNameInfo() throws SQLException { // Query1
                
                ResultSet rst = null;
                PreparedStatement getLongestLastNamesStmt = null;
                PreparedStatement getShortestLastNamesStmt = null;
                PreparedStatement getMostCommonLastNamesStmt = null;
                
                try {
                        //longest last names
                        String getLongestLastNamesSql = "SELECT DISTINCT (last_name) FROM " +
                        userTableName +
                        " WHERE LENGTH(last_name) = (SELECT max(LENGTH(last_name)) FROM " +
                        userTableName +
                        ")";
                        
                        getLongestLastNamesStmt = oracleConnection.prepareStatement(getLongestLastNamesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                        rst = getLongestLastNamesStmt.executeQuery();
                        
                        while(rst.next())
                        {
                                this.longestLastNames.add(rst.getString(1));
                        }
                        
                        //shortest last names
                        String getShortestLastNamesSql = "SELECT DISTINCT (last_name) FROM " +
                        userTableName +
                        " WHERE LENGTH(last_name) = (SELECT min(LENGTH(last_name)) FROM " +
                        userTableName +
                        ")";
                        getShortestLastNamesStmt = oracleConnection.prepareStatement(getShortestLastNamesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                        rst = getShortestLastNamesStmt.executeQuery();
                        
                        while(rst.next())
                        {
                           	this.shortestLastNames.add(rst.getString(1));
                        }
                        
                        //most common last names
                        String getMostCommonLastNamesSql = "SELECT last_name, COUNT(*) FROM " +
                        userTableName +
                        " WHERE last_name is not null GROUP BY last_name ORDER BY count(last_name) DESC";
                        getMostCommonLastNamesStmt = oracleConnection.prepareStatement(getMostCommonLastNamesSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                        rst = getMostCommonLastNamesStmt.executeQuery();
                        
                        this.mostCommonLastNamesCount = 0;
                        
                    	int count = 0;
                        while(rst.next())
                        {
                        	if(rst.isFirst() || rst.getInt(2) == count)
                        	{
                        		this.mostCommonLastNames.add(rst.getString(1));
                        		count = rst.getInt(2);
                                this.mostCommonLastNamesCount = count;
                        	}
                        	else
                        	{
                        		break;
                        	}
                        }
                } catch (SQLException e) {
                        System.err.println(e.getMessage());
                        // can do more things here
                        
                        throw e;
                } finally {
                        // Close statement and result set
                        if(rst != null)
                                rst.close();
                        
                        if(getLongestLastNamesStmt != null)
                                getLongestLastNamesStmt.close();
                        
                        if(getShortestLastNamesStmt != null)
                                getShortestLastNamesStmt.close();
                        
                        if(getMostCommonLastNamesStmt != null)
                                getMostCommonLastNamesStmt.close();
                }
                
                
        // Find the following information from your database and store the information as shown
		/*this.longestLastNames.add("JohnJacobJingleheimerSchmidt");
		this.shortestLastNames.add("Ng");
		this.shortestLastNames.add("Fu");
		this.shortestLastNames.add("Wu");
		this.mostCommonLastNames.add("Wang");
		this.mostCommonLastNames.add("Smith");
		this.mostCommonLastNamesCount = 10;*/
	}
	
	
	@Override
	// ***** Query 2 *****
	// Find the user(s) who have strictly more than 80 friends in the network
	//
	// Be careful on this query!
	// Remember that if two users are friends, the friends table
	// only contains the pair of user ids once, subject to 
	// the constraint that user1_id < user2_id
	//
	public void popularFriends() throws SQLException {
		//Find the following information from your database and store the information as shown 
		// this.popularFriends.add(new UserInfo(10L, "Billy", "SmellsFunny"));
		// this.popularFriends.add(new UserInfo(11L, "Jenny", "BadBreath"));
		// this.countPopularFriends = 2;
		ResultSet rst = null; 
		PreparedStatement getFriendCountStmt = null;
		// // PreparedStatement getNamesMostMonthStmt = null;
		// // PreparedStatement getNamesLeastMonthStmt = null;
		  try{
		  	String getFriendCountSql = "select first_name, last_name, user_id from " +
		  		userTableName + 
		  		" where user_id in (select user1_id from ( select user1_id, user2_id from " +
		  		friendsTableName +
		  		" union select user2_id, user1_id from " +
		  		friendsTableName +
		  		") group by user1_id HAVING COUNT(*) > 80)";

		  	getFriendCountStmt = oracleConnection.prepareStatement(getFriendCountSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

		  	rst = getFriendCountStmt.executeQuery();
		  	this.countPopularFriends = 0;
		  	while(rst.next()) {
		  		String firstName = rst.getString(1);
				String lastName = rst.getString(2);
				Long uid = rst.getLong(3);
				this.popularFriends.add(new UserInfo(uid, firstName, lastName));
		  		this.countPopularFriends++;
		  	}
		  }
		  catch (SQLException e) {
		  	System.err.println(e.getMessage());
		// // 	// can do more things here
		  	throw e;		
		  }
		 finally {
       		if(rst != null) 
        		rst.close();
      
      		if(getFriendCountStmt != null)
        		getFriendCountStmt.close();
        }
	//	PreparedStatement getPopularFriendsStmt = null;
	}
	 
@Override
	// ***** Query 3 *****
	// Find the users who still live in their hometowns
	// (I.e., current_city = hometown_city)
	//	
	public void liveAtHome() throws SQLException {
                
                
                ResultSet rst = null;
                PreparedStatement getLiveAtHomeStmt = null;
                
                try {
                        //longest last names
                        String getLiveAtHomeSql = "SELECT u.user_id, u.first_name, u.last_name FROM " +
                        userTableName + " U, " + currentCityTableName + " C, " + hometownCityTableName + " H " +
                        "WHERE c.current_city_id = h.hometown_city_id AND u.user_id = c.user_id AND u.user_id = h.user_id";
                        
                        getLiveAtHomeStmt = oracleConnection.prepareStatement(getLiveAtHomeSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                        rst = getLiveAtHomeStmt.executeQuery();
                        
                        while(rst.next()){
                                String firstName = rst.getString(2);
                                String lastName = rst.getString(3);
                                Long uid = rst.getLong(1);
                                this.liveAtHome.add(new UserInfo(uid, firstName, lastName));
                                ++this.countLiveAtHome;
                        }
                        
                } catch (SQLException e) {
                        System.err.println(e.getMessage());
                        // can do more things here
                        
                        throw e;
                } finally {
                        // Close statement and result set
                        if(rst != null)
                                rst.close();
                        
                        if(getLiveAtHomeStmt != null)
                                getLiveAtHomeStmt.close();
                }
	}



	@Override
	// **** Query 4 ****
	// Find the top-n photos based on the number of tagged users
	// If there are ties, choose the photo with the smaller numeric PhotoID first
	// 
	public void findPhotosWithMostTags(int n) throws SQLException { 
		// String photoId = "1234567";
		// String albumId = "123456789";
		// String albumName = "album1";
		// String photoCaption = "caption1";
		// String photoLink = "http://google.com";
		// PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
		// TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
		// tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName1", "taggedUserLastName1"));
		// tp.addTaggedUser(new UserInfo(12345L, "taggedUserFirstName2", "taggedUserLastName2"));
		// this.photosWithMostTags.add(tp);
		ResultSet tst = null; 
		ResultSet pst = null;
		ResultSet ust = null;
		PreparedStatement tagPhotoStmt = null;
		PreparedStatement photoInfoStmt = null;
		PreparedStatement usersStmt = null;
		try {
			String tagPhotoSQL = "SELECT tag_photo_id"+ 
			" FROM " + 
			tagTableName + 
			" GROUP BY tag_photo_id ORDER BY COUNT(tag_photo_id) DESC, tag_photo_id ASC";
			tagPhotoStmt = oracleConnection.prepareStatement(tagPhotoSQL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			tst = tagPhotoStmt.executeQuery();
			int i = n;
			while(tst.next() && n !=0 ) {
				String photoInfoSQL = "SELECT P.photo_id, A.album_id, A.album_name, P.photo_caption, P.photo_link FROM " + 
					 photoTableName + 
					 " P, " + 
					 albumTableName + 
					 " A WHERE P.photo_id = ? AND P.album_id=A.album_id";
				photoInfoStmt = oracleConnection.prepareStatement(photoInfoSQL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				String tid = tst.getString(1);
				photoInfoStmt.setString(1, tid);
				pst = photoInfoStmt.executeQuery();
				while (pst.next()){
					String pid = pst.getString(1);
					String aid = pst.getString(2);
					String albumName = pst.getString(3);
					String photoCaption = pst.getString(4);
					String photoLink = pst.getString(5);
					PhotoInfo x = new PhotoInfo(pid, aid, albumName, photoCaption, photoLink);
					TaggedPhotoInfo taggedPhoto = new TaggedPhotoInfo(x);
					
					String getTaggedUsersSql = "SELECT user_id, first_name, last_name FROM " + 
							userTableName + 
							" WHERE user_id IN (SELECT tag_subject_id FROM " + 
							tagTableName + 
							" WHERE tag_photo_id = ?)";
							//Question mark dynamic

					usersStmt = oracleConnection.prepareStatement(getTaggedUsersSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					usersStmt.setString(1, tid);
					//Don't know this but they did it in query 0
					ust = usersStmt.executeQuery();
					
					while (ust.next()){
						Long uid = ust.getLong(1);
						String firstName = ust.getString(2);
						String lastName = ust.getString(3);
						taggedPhoto.addTaggedUser(new UserInfo(uid, firstName, lastName));
					}
					
					this.photosWithMostTags.add(taggedPhoto);
				}
				n--;
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			throw e;		
		} finally {
			if(ust != null) 
				ust.close();
			if(tst != null) 
				tst.close();
			if(pst != null) 
				pst.close();
			if(photoInfoStmt != null)
				photoInfoStmt.close();
			if(usersStmt != null)
				usersStmt.close();
			if(tagPhotoStmt != null)
				tagPhotoStmt.close();

		}
	}


	
	
	
	@Override
	// **** Query 5 ****
	// Find suggested "match pairs" of friends, using the following criteria:
	// (1) One of the friends is female, and the other is male
	// (2) Their age difference is within "yearDiff"
	// (3) They are not friends with one another
	// (4) They should be tagged together in at least one photo
	//
	// You should up to n "match pairs"
	// If there are more than n match pairs, you should break ties as follows:
	// (i) First choose the pairs with the largest number of shared photos
	// (ii) If there are still ties, choose the pair with the smaller user_id for the female
	// (iii) If there are still ties, choose the pair with the smaller user_id for the male
	//
	public void matchMaker(int n, int yearDiff) throws SQLException { 
		Long girlUserId = 123L;
		String girlFirstName = "girlFirstName";
		String girlLastName = "girlLastName";
		int girlYear = 1988;
		Long boyUserId = 456L;
		String boyFirstName = "boyFirstName";
		String boyLastName = "boyLastName";
		int boyYear = 1986;
		MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName, 
				girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
		String sharedPhotoId = "12345678";
		String sharedPhotoAlbumId = "123456789";
		String sharedPhotoAlbumName = "albumName";
		String sharedPhotoCaption = "caption";
		String sharedPhotoLink = "link";
		mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
				sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
		this.bestMatches.add(mp);
	}

	
	
	// **** Query 6 ****
	// Suggest friends based on mutual friends
	// 
	// Find the top n pairs of users in the database who share the most
	// friends, but such that the two users are not friends themselves.
	//
	// Your output will consist of a set of pairs (user1_id, user2_id)
	// No pair should appear in the result twice; you should always order the pairs so that
	// user1_id < user2_id
	//
	// If there are ties, you should give priority to the pair with the smaller user1_id.
	// If there are still ties, give priority to the pair with the smaller user2_id.
	//
	@Override
	public void suggestFriendsByMutualFriends(int n) throws SQLException {
		// Long user1_id = 123L;
		// String user1FirstName = "Friend1FirstName";
		// String user1LastName = "Friend1LastName";
		// Long user2_id = 456L;
		// String user2FirstName = "Friend2FirstName";
		// String user2LastName = "Friend2LastName";
		// FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

		// p.addSharedFriend(567L, "sharedFriend1FirstName", "sharedFriend1LastName");
		// p.addSharedFriend(678L, "sharedFriend2FirstName", "sharedFriend2LastName");
		// p.addSharedFriend(789L, "sharedFriend3FirstName", "sharedFriend3LastName");
		// this.suggestedFriendsPairs.add(p);
		ResultSet rst = null; 
		Statement stmt = null;
try{
	 stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);		

    	 rst = stmt.executeQuery("SELECT mutualFriends.user1, U1.first_name, U1.last_name, mutualFriends.user2, U2.first_name, U2.last_name, mutualList.user3, U3.first_name, U3.last_name, mutualFriends.count "
     		  + "FROM ( "
          	  + "SELECT mutual.person1 as user1, mutual.person2 as user2, count(*) as count FROM "
              + "(SELECT F1.user1_id as person1, F2.user1_id as person2, F2.user2_id as person3 "
              + "FROM " 
              + friendsTableName 
              + " F1, " 
              + friendsTableName 
              + " F2 "
              + "WHERE F1.user2_id=F2.user2_id AND F1.user1_id!=F2.user1_id "
              + "UNION ALL "
              + "SELECT F1.user1_id as person1, F2.user2_id as person2, F2.user1_id as person3 "
              + "FROM " 
              + friendsTableName 
              + " F1, " 
              + friendsTableName 
              + " F2 "
              + "WHERE F1.user2_id=F2.user1_id AND F1.user1_id!=F2.user2_id "
              + "UNION ALL "
              + "SELECT F1.user2_id as person1, F2.user2_id as person2, F2.user1_id as person3 "
              + "FROM " 
              + friendsTableName 
              + " F1, " 
              + friendsTableName 
              + " F2 "
              + "WHERE F1.user1_id=F2.user1_id AND F1.user2_id!=F2.user2_id "
              + "ORDER BY 3 ASC) mutual "
          	  + "GROUP BY mutual.person1, mutual.person2 ORDER BY 3 DESC) mutualFriends "
      		  + "FULL OUTER JOIN ( "
          	  + "SELECT F1.user1_id as user1, F2.user1_id as user2, F2.user2_id as user3 "
              + "FROM " 
              + friendsTableName 
              + " F1, " 
              + friendsTableName 
              + " F2 "
              + "WHERE F1.user2_id=F2.user2_id AND F1.user1_id!=F2.user1_id "
              + "UNION ALL "
              + "SELECT F1.user1_id, F2.user2_id, F2.user1_id "
              + "FROM " 
              + friendsTableName 
              + " F1, " 
              + friendsTableName 
              + " F2 "
              + "WHERE F1.user2_id=F2.user1_id AND F1.user1_id!=F2.user2_id "
              + "UNION ALL "
              + "SELECT F1.user2_id, F2.user2_id, F2.user1_id "
              + "FROM " 
              + friendsTableName 
              + " F1, " 
              + friendsTableName 
              + " F2 "
              + "WHERE F1.user1_id=F2.user1_id AND F1.user2_id!=F2.user2_id "
              + "ORDER BY 3 ASC) mutualList "
     		  + "ON mutualFriends.user1=mutualList.user1 AND mutualFriends.user2=mutualList.user2 "
     		  + "FULL OUTER JOIN " 
     		  + userTableName 
     		  + " U1 ON U1.user_id=mutualFriends.user1 "
     		  + "FULL OUTER JOIN " 
     		  + userTableName 
     		  + " U2 ON U2.user_id=mutualFriends.user2 "
     		  + "FULL OUTER JOIN " 
     		  + userTableName 
     		  + " U3 ON U3.user_id=mutualList.user3 "
     		  + "WHERE mutualFriends.user1<mutualFriends.user2 AND mutualFriends.user1 NOT IN ( "
              + "SELECT F.user1_id FROM " 
              + friendsTableName 
              + " F "
              + "WHERE (mutualFriends.user1=F.user1_id AND mutualFriends.user2=F.user2_id)) "
     		  + "ORDER BY mutualFriends.count DESC, mutualFriends.user1 ASC, mutualFriends.user2 ASC, mutualList.user3 ASC"
    );

    Long base1_id = null;
    Long base2_id = null;
    rst.next();
    for(int i = 0; i < n; i++) {
      // Get friend pair
		  Long user1_id = rst.getLong(1);
	    String user1FirstName = rst.getString(2);
	    String user1LastName = rst.getString(3);
	    Long user2_id = rst.getLong(4);
	    String user2FirstName = rst.getString(5);
	    String user2LastName = rst.getString(6);
	    FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
      
      base1_id = user1_id;
      base2_id = user2_id;

      // Get shared friends
      do {
        user1_id = rst.getLong(1);
        user2_id = rst.getLong(4);
        if(!user1_id.equals(base1_id) || !user2_id.equals(base2_id)) break;

        Long sharedFriendId = rst.getLong(7);
        String sharedFriendFirstName = rst.getString(8);
        String sharedFriendLastName = rst.getString(9);
		    p.addSharedFriend(sharedFriendId, sharedFriendFirstName, sharedFriendLastName);
		    this.suggestedFriendsPairs.add(p);
      } while(rst.next());
    }
}
				catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
		// Close statement and result set
		rst.close();
		stmt.close();
		}


	}
	
	
//@Override
	// ***** Query 7 *****
	// Given the ID of a user, find information about that
	// user's oldest friend and youngest friend
	// 
	// If two users have exactly the same age, meaning that they were born
	// on the same day, then assume that the one with the larger user_id is older
	//
	public void findAgeInfo(Long user_id) throws SQLException {
                PreparedStatement getFriendsStmt = null;
                ResultSet rst = null;
                try {
                        //longest last names
                        String getFriendsSql = "SELECT user_id, first_name, last_name, year_of_birth FROM " +
                        userTableName +
                        " u JOIN (SELECT friend_table.user1_id, friend_table.user2_id FROM " +
                        friendsTableName +
                        " friend_table WHERE friend_table.user1_id = " +
                        user_id +
                        " OR friend_table.user2_id = " +
                        user_id +
                        ") all_friends ON all_friends.user1_id = u.user_id ORDER BY year_of_birth";
                        
                        getFriendsStmt = oracleConnection.prepareStatement(getFriendsSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                        rst = getFriendsStmt.executeQuery();
                        
                        while(rst.next()) {
                                if (rst.isFirst())
                                {
                                        Long uid = rst.getLong(1);
                                        String first_name = rst.getString(2);
                                        String last_name = rst.getString(3);
                                        this.oldestFriend = new UserInfo(uid, first_name, last_name);
                                }
                                if (rst.isLast())
                                {
                                        Long uid = rst.getLong(1);
                                        String first_name = rst.getString(2);
                                        String last_name = rst.getString(3);
                                        this.youngestFriend = new UserInfo(uid, first_name, last_name);
                                }
                        }
                        
                        //shortest last names
                        
                } catch (SQLException e) {
                        System.err.println(e.getMessage());
                        // can do more things here
                        
                        throw e;
                } finally {
                        // Close statement and result set
                        if(rst != null)
                                rst.close();
                        
                        if(getFriendsStmt != null)
                                getFriendsStmt.close();
                }
	}
	
	
	@Override
	// ***** Query 8 *****
	// 
	// Find the name of the city with the most events, as well as the number of 
	// events in that city.  If there is a tie, return the names of all of the (tied) cities.
	//
	public void findEventCities() throws SQLException {
		// this.eventCount = 12;
		// this.popularCityNames.add("Ann Arbor");
		// this.popularCityNames.add("Ypsilanti");
		ResultSet rst = null; 
		PreparedStatement getEventCountStmt = null;
		// PreparedStatement getNamesMostMonthStmt = null;
		// PreparedStatement getNamesLeastMonthStmt = null;

		try{
			String getEventCountSql = "select count(e.event_city_id), c.city_name FROM " + 
				eventTableName +
				" e, " +
				cityTableName +
				" c where e.event_city_id = c.city_id GROUP BY c.city_name HAVING count (e.event_id) = (SELECT MAX (count(*)) FROM "+
				eventTableName + " GROUP BY event_city_id)";

			getEventCountStmt = oracleConnection.prepareStatement(getEventCountSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			rst = getEventCountStmt.executeQuery();

			while(rst.next()) {
				this.eventCount = rst.getInt(1);
				this.popularCityNames.add(rst.getString(2));
			}

		}
		catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			
			if(getEventCountStmt != null)
				getEventCountStmt.close();
		}
	}
	
	
	
	@Override
//	 ***** Query 9 *****
	//
	// Find pairs of potential siblings and print them out in the following format:
	//   # pairs of siblings
	//   sibling1 lastname(id) and sibling2 lastname(id)
	//   siblingA lastname(id) and siblingB lastname(id)  etc.
	//
	// A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
	// if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
	// on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
	//  
	//
	public void findPotentialSiblings() throws SQLException {
		// Long user1_id = 123L;
		// String user1FirstName = "Friend1FirstName";
		// String user1LastName = "Friend1LastName";
		// Long user2_id = 456L;
		// String user2FirstName = "Friend2FirstName";
		// String user2LastName = "Friend2LastName";
		// SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
		// this.siblings.add(s);
		ResultSet rst = null; 
		PreparedStatement findSiblingPairStmt = null;
		try{
			String findSiblingPairSQL = "SELECT s1.user_id, s2.user_id, s1.last_name, s2.last_name, s1.first_name, s2.first_name FROM "+
			userTableName +
			" s1, " +
			userTableName +
			" s2, " +
			hometownCityTableName + 
			" HT1, " +
			hometownCityTableName +
			" HT2 WHERE s1.last_name = s2.last_name and HT1.hometown_city_id = HT2.hometown_city_id AND s1.user_id = HT1.user_id AND s2.user_id = HT2.user_id " + 
			" AND s1.user_id IN ( SELECT U.user1_id FROM " +
			friendsTableName + 
			" U WHERE U.user2_id = S2.user_id UNION  SELECT U.user2_id as user_id FROM " +
			friendsTableName +
			" U WHERE u.user1_id = s2.user_id) and ABS(S1.year_of_birth - S2.year_of_birth) < 10 and S1.user_id < S2.user_id order by S1.user_id, S2.user_id";
			
			findSiblingPairStmt = oracleConnection.prepareStatement(findSiblingPairSQL, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rst = findSiblingPairStmt.executeQuery();
			while(rst.next()) {
				Long uid1 = rst.getLong(1);
				String fname1 = rst.getString(5);
				String lname1 = rst.getString(3);
				Long uid2 = rst.getLong(2);
				String fname2 = rst.getString(6);
				String lname2=rst.getString(4);

				this.siblings.add(new SiblingInfo(uid1,fname1,lname1,uid2,fname2,lname2));
			}

		}
		catch (SQLException e) {
			System.err.println(e.getMessage());
			// can do more things here
			throw e;		
		} finally {
			// Close statement and result set
			if(rst != null) 
				rst.close();
			if(findSiblingPairStmt != null)
				findSiblingPairStmt.close();
		}
	}
}
