package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    // (B) Find the birth month in which the most users were born
    // (C) Find the birth month in which the fewest users (at least one) were born
    // (D) Find the IDs, first names, and last names of users born in the month
    // identified in (B)
    // (E) Find the IDs, first names, and last name of users born in the month
    // identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find
    // the appropriate
    // mechanisms for opening up a statement, executing a query, walking through
    // results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that
                                                                    // birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month,
                                                                          // descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); // it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); // it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    // (B) The first name(s) with the fewest letters
    // (C) The first name held by the most users
    // (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * FirstNameInfo info = new FirstNameInfo();
             * info.addLongName("Aristophanes");
             * info.addLongName("Michelangelo");
             * info.addLongName("Peisistratos");
             * info.addShortName("Bob");
             * info.addShortName("Sue");
             * info.addCommonName("Harold");
             * info.addCommonName("Jessica");
             * info.setCommonNameCount(42);
             * return info;
             */

            FirstNameInfo info = new FirstNameInfo();
            // (A)
            ResultSet rst = stmt.executeQuery(
                    "SELECT DISTINCT First_Name " +
                            " FROM " + UsersTable + " " +
                            "WHERE First_Name IS NOT NULL " +
                            "ORDER BY LENGTH(First_Name) DESC, First_Name ASC");

            Integer longestSize = 0;
            while (rst.next()) {
                if (rst.isFirst()) {
                    longestSize = rst.getString(1).length();
                }
                if (rst.getString(1).length() == longestSize) {
                    info.addLongName(rst.getString(1));
                }
            }

            // (B)
            rst = stmt.executeQuery(
                    "SELECT DISTINCT First_Name " +
                            " FROM " + UsersTable + " " +
                            "WHERE First_Name IS NOT NULL " +
                            "ORDER BY LENGTH(First_Name) ASC, First_Name ASC");

            Integer shortestSize = 0;

            while (rst.next()) {
                if (rst.isFirst()) {
                    shortestSize = rst.getString(1).length();
                }
                if (rst.getString(1).length() == shortestSize) {
                    info.addShortName(rst.getString(1));
                }
            }

            // (C)
            rst = stmt.executeQuery(
                    "SELECT First_Name " +
                            " FROM " + UsersTable + " " +
                            "WHERE First_Name IS NOT NULL " +
                            "GROUP BY First_Name " +
                            "ORDER BY COUNT(*) DESC");

            String name = "";
            rst.first();
            name = rst.getString(1);

            info.addCommonName(name);

            // (D)
            rst = stmt.executeQuery(
                    "SELECT First_Name, COUNT(*) " +
                            " FROM " + UsersTable + " " +
                            "GROUP BY First_Name " +
                            "ORDER BY COUNT(*) DESC");
            Integer count = 0;
            rst.first();
            count = rst.getInt(2);
            info.setCommonNameCount(count);

            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any
    // friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only
    // contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
             * UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
             * results.add(u1);
             * results.add(u2);
             */

            ResultSet rst = stmt.executeQuery(
                    "SELECT u1.User_ID, u1.First_Name, u1.Last_Name FROM " + UsersTable + " u1 " +
                            "JOIN " +
                            "(SELECT DISTINCT User_ID " +
                            " FROM " + UsersTable + " " +
                            "MINUS " +
                            "(SELECT DISTINCT User1_ID as User_ID FROM " +
                            FriendsTable + " " +
                            "UNION " +
                            "SELECT DISTINCT User2_ID as User_ID FROM " +
                            FriendsTable + ")" + ") u2 ON u1.User_ID = u2.User_ID " +
                            "ORDER BY u1.User_ID ASC");

            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                results.add(u1);
            }

            rst.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer
    // live
    // in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
             * UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
             * results.add(u1);
             * results.add(u2);
             */

            ResultSet rst = stmt.executeQuery(
                    "SELECT u1.User_ID, u1.First_Name, u1.Last_Name FROM " + UsersTable + " u1 JOIN " +
                            "(SELECT DISTINCT c.User_ID as User_ID FROM " + CurrentCitiesTable + " c JOIN "
                            + HometownCitiesTable +
                            " h ON c.User_ID = h.User_ID WHERE c.current_city_id != h.hometown_city_id AND c.current_city_id IS NOT NULL AND "
                            +
                            "h.hometown_city_id IS NOT NULL) u2 ON u1.User_ID = u2.User_ID " +
                            "ORDER BY u1.User_ID ASC");

            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                results.add(u1);
            }
            rst.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of
    // the top
    // <num> photos with the most tagged users
    // (B) For each photo identified in (A), find the IDs, first names, and last
    // names
    // of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
             * UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
             * UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
             * UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
             * TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
             * tp.addTaggedUser(u1);
             * tp.addTaggedUser(u2);
             * tp.addTaggedUser(u3);
             * results.add(tp);
             */

            // Find photos
            ResultSet rst = stmt.executeQuery(
                    "SELECT p.photo_id, p.album_id, p.photo_link, a.album_name FROM " + PhotosTable + " p JOIN " +
                            "(SELECT tag_photo_id as photo_id, count(*) as count FROM " + TagsTable +
                            " GROUP BY tag_photo_id ORDER BY count(*) DESC, tag_photo_id ASC) t ON p.photo_id = t.photo_id"
                            +
                            " JOIN " + AlbumsTable + " a ON p.album_id = a.album_id WHERE ROWNUM <=" + num);

            try (Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll,
                    FakebookOracleConstants.ReadOnly)) {
                while (rst.next()) {
                    int photoID = rst.getInt(1);
                    PhotoInfo p = new PhotoInfo(photoID, rst.getInt(2), rst.getString(3), rst.getString(4));

                    // find users

                    ResultSet rst2 = stmt2.executeQuery(
                            "SELECT u.User_ID, u.First_Name, u.Last_Name FROM " + UsersTable + " u JOIN " + TagsTable
                                    + " t ON t.tag_subject_id = u.User_ID WHERE t.tag_photo_id = " + photoID
                                    + " ORDER BY u.User_ID ASC");
                    TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                    // add users
                    while (rst2.next()) {
                        UserInfo u1 = new UserInfo(rst2.getInt(1), rst2.getString(2), rst2.getString(3));
                        tp.addTaggedUser(u1);
                    }
                    results.add(tp);
                }
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }
            // while (rst.next()) {
            // int photoID = rst.getInt(1);
            // PhotoInfo p = new PhotoInfo(photoID, rst.getInt(2), rst.getString(3),
            // rst.getString(4));

            // // find users

            // ResultSet rst2 = stmt.executeQuery(
            // "SELECT u.User_ID, u.First_Name, u.Last_Name FROM " + UsersTable + " u JOIN "
            // + TagsTable
            // + " t ON t.tag_subject_id = u.User_ID WHERE t.tag_photo_id = " + photoID
            // + " ORDER BY u.User_ID ASC");
            // TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
            // // add users
            // while (rst2.next()) {
            // UserInfo u1 = new UserInfo(rst2.getInt(1), rst2.getString(2),
            // rst2.getString(3));
            // tp.addTaggedUser(u1);
            // }
            // results.add(tp);
            // }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of
    // the two
    // users in the top <num> pairs of users that meet each of the following
    // criteria:
    // (i) same gender
    // (ii) tagged in at least one common photo
    // (iii) difference in birth years is no more than <yearDiff>
    // (iv) not friends
    // (B) For each pair identified in (A), find the IDs, links, and IDs and names
    // of
    // the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
             * UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
             * MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
             * PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
             * mp.addSharedPhoto(p);
             * results.add(mp);
             */

            ResultSet rst = stmt.executeQuery(
                    "SELECT u1.user_id as user1_id, u1.First_Name as user1_First_Name, u1.Last_Name as user1_Last_Name, u1.year_of_birth as user1_year_of_birth, u2.user_id as user2_id, u2.First_Name as user2_First_Name, u2.Last_Name as user2_Last_Name, u2.year_of_birth as user2_year_of_birth FROM "
                            +
                            "(SELECT t.user1_id, t.user2_id FROM " +
                            "(SELECT u1.user_id as user1_id, u2.user_id as user2_id FROM " + UsersTable + " u1, "
                            + UsersTable + " u2 " +
                            "WHERE u1.gender = u2.gender AND ABS(u1.year_of_birth - u2.year_of_birth) <= " + yearDiff +
                            " INTERSECT " +
                            "SELECT u1.tag_subject_id as user1_id, u2.tag_subject_id as user2_id FROM " + TagsTable
                            + " u1 JOIN " + TagsTable +
                            " u2 ON u1.tag_photo_id = u2.tag_photo_id WHERE u1.tag_subject_id < u2.tag_subject_id " +
                            " INTERSECT " +
                            " SELECT u1.user_id as user1_id, u2.user_id as user2_id FROM " + UsersTable + " u1, "
                            + UsersTable + " u2 " +
                            " WHERE u1.user_id < u2.user_id MINUS SELECT user1_id, user2_id FROM " + FriendsTable
                            + ") t," + TagsTable +
                            " t1, " + TagsTable
                            + " t2 WHERE t1.tag_photo_id = t2.tag_photo_id AND t1.tag_subject_id = t.user1_id AND " +
                            "  t2.tag_subject_id = t.user2_id GROUP BY t.user1_id, t.user2_id ORDER BY COUNT(*) DESC) tt"
                            +

                            " JOIN " + UsersTable + " u1 ON u1.user_id = tt.user1_id JOIN " + UsersTable
                            + " u2 ON u2.user_id = tt.user2_id " +
                            "WHERE ROWNUM <= " + num + " ORDER BY u1.user_id ASC, u2.user_id ASC");

            // "SELECT u1.user_id as user1_id, u1.First_Name as user1_First_Name,
            // u1.Last_Name as user1_Last_Name, u1.year_of_birth as user1_year_of_birth,
            // u2.user_id as user2_id, u2.First_Name as user2_First_Name, u2.Last_Name as
            // user2_Last_Name, u2.year_of_birth as user2_year_of_birth FROM "
            // +
            // "(SELECT u1.user_id as user1_id, u2.user_id as user2_id FROM " + UsersTable +
            // " u1, "
            // + UsersTable + " u2 " +
            // "WHERE u1.gender = u2.gender AND ABS(u1.year_of_birth - u2.year_of_birth) <=
            // " + yearDiff +
            // " INTERSECT " +
            // "SELECT u1.tag_subject_id as user1_id, u2.tag_subject_id as user2_id FROM " +
            // TagsTable
            // + " u1 JOIN " + TagsTable +
            // " u2 ON u1.tag_photo_id = u2.tag_photo_id WHERE u1.tag_subject_id <
            // u2.tag_subject_id " +
            // " INTERSECT " +
            // " SELECT u1.user_id as user1_id, u2.user_id as user2_id FROM " + UsersTable +
            // " u1, "
            // + UsersTable + " u2 " +
            // " WHERE u1.user_id < u2.user_id MINUS SELECT user1_id, user2_id FROM " +
            // FriendsTable
            // + ") t" +
            // " JOIN " + UsersTable + " u1 ON u1.user_id = t.user1_id JOIN " + UsersTable
            // + " u2 ON u2.user_id = t.user2_id " +
            // "WHERE ROWNUM <= " + num + " ORDER BY u1.user_id ASC, u2.user_id ASC");

            try (Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll,
                    FakebookOracleConstants.ReadOnly)) {
                while (rst.next()) {
                    Integer user1ID = rst.getInt(1);
                    UserInfo u1 = new UserInfo(user1ID, rst.getString(2), rst.getString(3));
                    Integer user2ID = rst.getInt(5);
                    UserInfo u2 = new UserInfo(user2ID, rst.getString(6), rst.getString(7));
                    MatchPair mp = new MatchPair(u1, rst.getInt(4), u2, rst.getInt(8));

                    ResultSet rst2 = stmt2.executeQuery(
                            "SELECT p.photo_id, p.album_id, p.photo_link, a.album_name FROM " + TagsTable + " u1 " +
                                    " JOIN " + TagsTable
                                    + " u2 ON u1.tag_photo_id = u2.tag_photo_id AND u1.tag_subject_id = " + user1ID +
                                    " AND u2.tag_subject_id = " + user2ID + " JOIN " + PhotosTable
                                    + " p ON p.photo_id = u1.tag_photo_id " +
                                    " JOIN " + AlbumsTable + " a ON a.album_id = p.album_id");
                    while (rst2.next()) {
                        PhotoInfo p = new PhotoInfo(rst2.getInt(1), rst2.getInt(2), rst2.getString(3),
                                rst2.getString(4));
                        mp.addSharedPhoto(p);
                    }
                    results.add(mp);
                }
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users
    // in
    // the top <num> pairs of users who are not friends but have a lot of
    // common friends
    // (B) For each pair identified in (A), find the IDs, first names, and last
    // names
    // of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(16, "The", "Hacker");
             * UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
             * UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
             * UsersPair up = new UsersPair(u1, u2);
             * up.addSharedFriend(u3);
             * results.add(up);
             */

            // FIND MUTUAL FRIEND PAIRS

            // create View temp
            Integer val = stmt.executeUpdate(
                    "CREATE VIEW temp AS SELECT user1_id, user2_id FROM " + FriendsTable +
                            " UNION SELECT user2_id, user1_id FROM " + FriendsTable);

            ResultSet rst = stmt.executeQuery(
                    "SELECT u1.user_ID as user1_id, u1.First_Name as user1_First_Name, u1.Last_Name as user1_Last_Name, u2.user_id as user2_id, u2.First_Name as user2_First_Name, u2.Last_Name as user2_Last_Name FROM "
                            +
                            "(SELECT t.user1_id as user1_id, t.user2_id as user2_id, COUNT(*) as count FROM " +
                            " (SELECT t1.user1_id as user1_id, t2.user2_id as user2_id FROM temp t1, " +
                            "temp t2 WHERE t1.user2_id = t2.user1_id AND t1.user1_id != t2.user2_id) t GROUP BY t.user1_id, t.user2_id ORDER BY count DESC, "
                            +
                            " t.user1_id ASC, t.user2_id ASC) tt JOIN " + UsersTable +
                            " u1 ON u1.user_id = tt.user1_id JOIN " + UsersTable +
                            " u2 ON u2.user_id = tt.user2_id WHERE u1.user_id < u2.user_id AND ROWNUM <= " + num);

            try (Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll,
                    FakebookOracleConstants.ReadOnly)) {
                while (rst.next()) {
                    Integer mut1 = rst.getInt(1);
                    Integer mut2 = rst.getInt(4);
                    UserInfo u1 = new UserInfo(mut1, rst.getString(2), rst.getString(3));
                    UserInfo u2 = new UserInfo(mut2, rst.getString(5), rst.getString(6));
                    UsersPair up = new UsersPair(u1, u2);

                    ResultSet rst2 = stmt2.executeQuery(
                            "SELECT u.user_id, u.First_Name, u.Last_Name FROM " +
                                    " (SELECT DISTINCT t1.user2_id as user_id FROM temp t1, temp t2 WHERE t1.user2_id = t2.user1_id AND t1.user1_id = "
                                    + mut1 + " AND t2.user2_id = " + mut2 + ") t " +
                                    " JOIN " + UsersTable + " u ON u.user_id = t.user_id ORDER BY u.user_id ASC");
                    while (rst2.next()) {
                        UserInfo u3 = new UserInfo(rst2.getInt(1), rst2.getString(2), rst2.getString(3));
                        up.addSharedFriend(u3);
                    }
                    results.add(up);
                }
            } catch (SQLException e) {
                System.err.println(e.getMessage());
            }

            // drop view
            Integer val2 = stmt.executeUpdate(
                    "DROP VIEW temp");

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are
    // held
    // (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * EventStateInfo info = new EventStateInfo(50);
             * info.addState("Kentucky");
             * info.addState("Hawaii");
             * info.addState("New Hampshire");
             * return info;
             */
            // return new EventStateInfo(-1); // placeholder for compilation

            // create views
            ResultSet rst = stmt.executeQuery(
                    "SELECT t1.state_name, t1.count FROM (SELECT c.state_name as state_name, COUNT(*) as count FROM "
                            + CitiesTable + " c JOIN " + EventsTable + " e ON " +
                            "c.City_ID = e.Event_City_ID GROUP BY c.state_name) t1 JOIN " +
                            "(SELECT MAX(COUNT(*)) as count FROM " + CitiesTable + " c JOIN " + EventsTable +
                            " e ON c.City_ID = e.Event_City_ID GROUP BY c.state_name) t2 " +
                            "ON t1.count = t2.count ORDER BY t1.state_name ASC");

            rst.first();
            Integer eventCount = rst.getInt(2);
            EventStateInfo info = new EventStateInfo(eventCount);
            info.addState(rst.getString(1));
            while (rst.next()) {
                info.addState(rst.getString(1));
            }

            rst.close();
            stmt.close();
            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the
    // user
    // with User ID <userID>
    // (B) Find the ID, first name, and last name of the youngest friend of the user
    // with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
             * UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
             * return new AgeInfo(old, young);
             */
            // return new AgeInfo(new UserInfo(-1, "UNWRITTEN", "UNWRITTEN"), new
            // UserInfo(-1, "UNWRITTEN", "UNWRITTEN"));

            // find youngest
            ResultSet rst = stmt.executeQuery(
                    "SELECT u.User_ID, u.First_Name, u.Last_Name FROM " + UsersTable + " u JOIN " +
                            "(SELECT user1_id as user_id FROM " + FriendsTable + " WHERE user2_id = " + userID +
                            " UNION " +
                            "SELECT user2_id as user_id FROM " + FriendsTable + " WHERE user1_id = " + userID + ") t" +
                            " ON u.User_ID = t.user_id ORDER BY u.year_of_birth DESC, u.month_of_birth DESC, u.day_of_birth DESC, u.User_ID DESC");

            rst.first();
            UserInfo young = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));

            // find oldest
            rst = stmt.executeQuery(
                    "SELECT u.User_ID, u.First_Name, u.Last_Name FROM " + UsersTable + " u JOIN " +
                            "(SELECT user1_id as user_id FROM " + FriendsTable + " WHERE user2_id = " + userID +
                            " UNION " +
                            "SELECT user2_id as user_id FROM " + FriendsTable + " WHERE user1_id = " + userID + ") t" +
                            " ON u.User_ID = t.user_id ORDER BY u.year_of_birth ASC, u.month_of_birth ASC, u.day_of_birth ASC, u.User_ID DESC");

            rst.first();
            UserInfo old = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));

            rst.close();
            stmt.close();

            return new AgeInfo(old, young);

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }

    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    // (i) same last name
    // (ii) same hometown
    // (iii) are friends
    // (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            /*
             * EXAMPLE DATA STRUCTURE USAGE
             * ============================================
             * UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
             * UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
             * SiblingInfo si = new SiblingInfo(u1, u2);
             * results.add(si);
             */

            ResultSet rst = stmt.executeQuery(
                    "SELECT user1.User_ID as User1_ID, user1.First_Name as User1_First_Name, user1.Last_Name as User1_Last_Name, user2.User_ID as User2_ID, user2.First_Name as User2_First_Name, user2.Last_Name as User2_Last_Name "
                            +
                            "FROM " +
                            "(SELECT u1.User_ID as User1_ID, u2.User_ID as User2_ID FROM " + UsersTable + " u1 JOIN "
                            + UsersTable +
                            " u2 ON u1.Last_Name = u2.Last_Name WHERE ABS(u1.year_of_birth - u2.year_of_birth) < 10 "
                            +
                            "INTERSECT " +
                            "SELECT u1.User_ID as User1_ID, u2.User_ID as User2_ID FROM " + HometownCitiesTable
                            + " u1 JOIN "
                            + HometownCitiesTable + " u2 ON u1.hometown_city_id = u2.hometown_city_id " +
                            "INTERSECT " +
                            "SELECT User1_ID, User2_ID FROM " + FriendsTable + ") t " +
                            " JOIN " + UsersTable + " user1 ON user1.User_ID = t.User1_ID " +
                            " JOIN " + UsersTable + " user2 ON user2.User_ID = t.User2_ID " +
                            " ORDER BY user1_ID ASC, user2_ID ASC");

            while (rst.next()) {
                UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getInt(4), rst.getString(5), rst.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }
            rst.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
