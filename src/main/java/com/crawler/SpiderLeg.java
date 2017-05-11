package com.crawler;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by grantdeshazer on 4/29/17.
 *
 * Retrieves HTLM page, parses the page for links, and stores the links for filtering.
 *
 */
public class SpiderLeg {

    private Set<String> links = new HashSet<>();
    private Document htmlDocument;

    private static final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 " +
                                             "(KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1";

    public boolean crawl(String url){
        try{
            Connection connection = Jsoup.connect(url).userAgent(USER_AGENT);
            htmlDocument = connection.get();


            if(connection.response().statusCode() == 200){
//                System.out.println("Retrieved web page at " + url);

            } else if (connection.response().statusCode() != 200){
                System.err.println("Failed to retrieve page.  HTTP error: " + connection.response().statusCode());
                return false;
            }

            if(!connection.response().contentType().contains("text/html")){
                System.err.println("***FAILURE*** Retrieved something other than HTML");
                return false;
            }


            Elements linksOnPage = htmlDocument.select("a[href");
//            System.out.println("Found: " + linksOnPage.size() + " links");

            for(Element link : linksOnPage){
                this.links.add(link.absUrl("href"));
            }

            return true;

        } catch (Exception e){
            System.err.println("Error in http request" + e);
            return false;
        }
    }

    public List<String> getPages(){
        List<String> output = new LinkedList<>();
        output = links.stream().collect(Collectors.toList());
        return output;
    }
}
