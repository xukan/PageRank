package Reader;

/**
 * Created by Onerepublic on 4/20/17.
 */
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class Bz2WikiParser {
    private static Pattern namePattern = Pattern.compile("^([^~]+)$");
    private static Pattern linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    private Set<String> linkPageNames = new HashSet();

    public Bz2WikiParser() {
    }

    public XMLReader getXMLReader() {
        try {
            SAXParserFactory e = SAXParserFactory.newInstance();
            e.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = e.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            xmlReader.setContentHandler(new Bz2WikiParser.WikiParser(this.linkPageNames));
            return xmlReader;
        } catch (Exception var4) {
            var4.printStackTrace();
            return null;
        }
    }

    public Set<String> getLinkPageNames() {
        return this.linkPageNames;
    }

    public String[] linksArray() {
        String[] linkPages = new String[this.linkPageNames.size()];
        int i = 0;

        String page;
        for(Iterator var3 = this.linkPageNames.iterator(); var3.hasNext(); linkPages[i++] = page) {
            page = (String)var3.next();
        }

        return linkPages;
    }

    public String processLine(String line, XMLReader xmlReader) {
        int delimLoc = line.indexOf(58);
        if(delimLoc <= 0) {
            return "";
        } else {
            String pageName = line.substring(0, delimLoc);
            String html = line.substring(delimLoc + 1);
            Matcher matcher = namePattern.matcher(pageName);
            if(!matcher.find()) {
                return "";
            } else {
                this.linkPageNames.clear();

                try {
                    xmlReader.parse(new InputSource(new StringReader(html)));
                } catch (Exception var8) {
                    return "";
                }

                if(this.linkPageNames.contains(pageName)) {
                    this.linkPageNames.remove(pageName);
                }

                return pageName;
            }
        }
    }

    private static class WikiParser extends DefaultHandler {
        private Set<String> linkPageNames;
        private int count = 0;

        public WikiParser(Set<String> linkPageNames) {
            this.linkPageNames = linkPageNames;
        }

        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            if("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && this.count == 0) {
                this.count = 1;
            } else if(this.count > 0 && "a".equalsIgnoreCase(qName)) {
                ++this.count;
                String link = attributes.getValue("href");
                if(link == null) {
                    return;
                }

                try {
                    link = URLDecoder.decode(link, "UTF-8");
                } catch (Exception var7) {
                    ;
                }

                Matcher matcher = Bz2WikiParser.linkPattern.matcher(link);
                if(matcher.find()) {
                    this.linkPageNames.add(matcher.group(1));
                }
            } else if(this.count > 0) {
                ++this.count;
            }

        }

        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if(this.count > 0) {
                --this.count;
            }

        }
    }
}
