//DANIEL BASHARY
//WAS GETTING AROUND 15-22 minutes to find all the websites and add them to the database.

import java.net.URL;
import java.sql.*;
import java.util.*;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class EmailScraper {
    static final Set<String> email = Collections.synchronizedSet(new HashSet<>());
    static final Set<String> sites = Collections.synchronizedSet(new HashSet<>());
    static ArrayList<String> checkWebsiteToBlock = new ArrayList<>();
    static ArrayList<Integer> emailAmount = new ArrayList<>();
    static ArrayList<Integer> checkVisitAmount = new ArrayList<>();
    static ExecutorService pool = Executors.newFixedThreadPool(800);
    static final int FINALEMAILSIZE = 10_000;
    static HashSet<String> addedToDatabase = new HashSet<>();


    public static void BlockedListCreator(String website, int emailAdded, int visitAmount) {
        checkWebsiteToBlock.add(website);
        emailAmount.add(emailAdded);
        checkVisitAmount.add(visitAmount);
    }

    public static boolean emailChecker(String email) {
        Pattern pattern = Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}");
        Matcher mat = pattern.matcher(email);
        return mat.matches();
    }

    static void emailScraper(String url) {
        try {
            String blockCheck = new URL(url).getHost();
            int index = checkWebsiteToBlock.indexOf(blockCheck);
            if (index == -1 || checkVisitAmount.get(index) < 750 || emailAmount.get(index) > 4) {
                Document doc = Jsoup.connect(url).get();
                Elements links = doc.select("a[href]");
                for (Element link : links) {
                    String emailOrWebsite = link.attr("abs:href");
                    if (emailOrWebsite.contains("mailto:")) {
                        if (emailChecker(emailOrWebsite.substring(7))) {
                            email.add(emailOrWebsite.substring(7));
                            if (index == -1) {
                                BlockedListCreator(blockCheck, 1, 1);
                            } else {
                                emailAmount.add(index, emailAmount.get(index) + 1);
                            }
                        }
                    } else {
                        if (emailOrWebsite.contains(".edu") || emailOrWebsite.contains(".gov")) {
                            sites.add(emailOrWebsite);
                        }
                        if (index == -1) {
                            BlockedListCreator(blockCheck, 0, 1);
                        } else {
                            checkVisitAmount.add(index, checkVisitAmount.get(index) + 1);
                        }
                    }
                }
            } else { synchronized (sites) { sites.removeIf(s -> s.contains(url)); } }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class Scraper implements Runnable {
        final String url;

        Scraper(String url) {
            this.url = url;
        }

        @Override
        public void run() { emailScraper(url); }
    }

    static class DatabaseAdd implements Runnable {
        String connectionUrl =
                "jdbc:sqlserver://mco364.ckxf3a0k0vuw.us-east-1.rds.amazonaws.com;"
                        + "database=DanielBashary;"
                        + "user=admin364;"
                        + "password=mco364lcm;"
                        + "encrypt=false;"
                        + "trustServerCertificate=false;"
                        + "loginTimeout=30;";
        String insertSql = "INSERT INTO EmailScraper (Address) VALUES (?);";


        @Override
        public void run() {
            try (Connection connection = DriverManager.getConnection(connectionUrl)) {
                PreparedStatement prepsInsertProduct = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
                connection.setAutoCommit(false);
                synchronized (email){
                    for (String databaseValue : email) {
                        if (addedToDatabase.add(databaseValue)) {
                            prepsInsertProduct.setString(1, databaseValue);
                            prepsInsertProduct.addBatch();
                            if (addedToDatabase.size() >= FINALEMAILSIZE){
                                break;
                            }
                        }
                    }
                    prepsInsertProduct.executeBatch();
                    connection.commit();
                    if (addedToDatabase.size() >= FINALEMAILSIZE){
                        pool.shutdownNow();
                    }
                }
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Set<String> visitedSites = Collections.synchronizedSet(new HashSet<>());
        int databaseThreadStarter = 500;
        visitedSites.add("https://touro.edu");
        emailScraper("https://touro.edu");
        while (email.size() <= FINALEMAILSIZE && addedToDatabase.size() < FINALEMAILSIZE) {
            synchronized (sites) {
                Iterator<String> iterator = sites.iterator();
                while (iterator.hasNext()) {
                    String url = iterator.next();
                    iterator.remove();
                    if (visitedSites.add(url) && addedToDatabase.size() < FINALEMAILSIZE) { pool.execute(new Scraper(url)); }
                    if (email.size() != 0 && email.size() >= databaseThreadStarter  && addedToDatabase.size() < FINALEMAILSIZE) {
                        pool.execute(new DatabaseAdd());
                        databaseThreadStarter += 500;
                    }
                    if (email.size() >= FINALEMAILSIZE && addedToDatabase.size() <= FINALEMAILSIZE){ pool.execute(new DatabaseAdd()); }
                }
            }
        }
    }
}