package code.distcrawler.frontend;

import static spark.Spark.*;


import java.io.ByteArrayInputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.jsoup.Jsoup;
import org.jsoup.helper.W3CDom;
import org.jsoup.parser.Parser;
import org.jsoup.parser.XmlTreeBuilder;
import org.w3c.dom.Document;

import code.distcrawler.crawler.info.URLInfo;
import code.distcrawler.storage.ChannelKey;
import code.distcrawler.storage.ChannelValue;
import code.distcrawler.storage.DBWrapper;
import code.distcrawler.storage.PageKey;
import code.distcrawler.storage.PageValue;
import code.distcrawler.storage.UserKey;
import code.distcrawler.storage.UserValue;
import code.distcrawler.xpathengine.*;
import code.distcrawler.xpathengine.XPathEngineFactory;
import code.distcrawler.xpathengine.XPathEngineImpl;
import spark.Request;
import spark.Response;

class XPathApp {
	// This function return the hashed input using SHA-256
	public static String hash(String input) {

		String res = "";
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
			for (byte elem : hashedBytes) {
				res = res + Integer.toHexString(0xff & elem);
			}
		} catch (Exception e) {
			e.printStackTrace();
			res = null;
		}
		return res;
	}

	public static class UserChannelChecker {
		public String response_string = null;
		public String channelName = null;
		public DBWrapper db = null;
		public UserValue user = null;
		public UserKey userKey = null;
		public String username;
		public boolean check = false;

		public UserChannelChecker(Request request, Response response) {
			this.username = (String) (request.session().attribute("username"));
			if (this.username == null) {
				response.status(401);
				this.response_string = "<html><body> Error 401. No user is logged in <br><a href=\"/\">Main Page</a></body></html>";
				return;
			}
			this.channelName = request.queryParams("name");
			if (this.channelName == null) {
				response.status(400);
				this.response_string = "<html><body> Error 400. Bad Request. </body></html>";
				return;
			}
			this.db = new DBWrapper(null);
			this.userKey = new UserKey(this.username);
			this.user = db.getEntry(this.userKey);
			if (this.user == null) {
				response.redirect("/logout");
				return;
			}
			this.check = true;
			return;
		}
	}



	public static void main(String args[]) {

		// Read the input arguments
		if (args.length != 1) {
			System.err.println("You need to provide the path to the BerkeleyDB data store!");
			System.exit(1);
		}

		// Initialize Database and set the directory of the database enviroment
		new DBWrapper(args[0]);
		// Start the server listening at port 8080
		port(8080);

		// below is test for java dynamodb sdk
		
		// If the user has a session containing a user name show welcome message and a
		// list
		// of the different channels with the appropriate links for subscribing,
		// unsubscribing,
		// deleting channel and also a form for creating a channel.
		// Otherwise if no user is logged in show form to login, and link for new user
		// registration
		get("/", (request, response) -> {
			String name = (String) (request.session().attribute("username"));
			String firstName = (String) (request.session().attribute("firstName"));
			String lastName = (String) (request.session().attribute("lastName"));
			String attempts = (String) (request.session().attribute("attempts"));
			String title = "Please enter your user name:";
			if (attempts != null) {
				title = "Sorry wrong username or password please try again:";
			}
			if (name == null) {
				return "<html><body><b>" + title + "<form action=\"/login\" method=\"POST\">  <b>Username</b> <br>"
						+ "<input type=\"text\" placeholder=\"Enter Username\" name=\"name\" required> <br>"
						+ "<b>Password</b> <br> <input type=\"password\" placeholder=\"Enter Password\" name=\"password\" required> <br>"
						+ " <button type=\"submit\">Login</button>   </form>"
						+ "<a href=\"/newaccount\"> Create New Account</a></body></html>";
			}
			DBWrapper db = new DBWrapper(null);
			ArrayList<ChannelValue> channels = db.getChannels();
			String responseString = "<html><body><p>Welcome, " + firstName + " " + lastName
					+ "!</p><div><b>Available Channels:</b><ul>";
			UserKey userKey = new UserKey(name);
			UserValue user = db.getEntry(userKey);
			if (user == null) {
				response.redirect("/logout");
				return null;
			}
			for (ChannelValue channel : channels) {
				String channelName = channel.getName();
				boolean isSubscribed = user.getChannelSet().contains(channelName);
				boolean isCreator = channel.getCreator().contentEquals(user.getName());
				// System.out.println("Channe="+channelName+" User="+user.getName()+"
				// Creator="+channel.getCreator()+" isCreator="+isCreator);
				responseString = responseString + "<li>" + channelName;
				if (isSubscribed) {
					responseString = responseString + " (Subscribed in the channel) <a href=\"/unsubscribe?name="
							+ URLEncoder.encode(channelName, "UTF-8") + "\"> [Unsubscribe]</a>,"
							+ "<a href=\"/show?name=" + URLEncoder.encode(channelName, "UTF-8") + "\">[Show]</a>";
					if (isCreator) {
						responseString = responseString + "<a href=\"/delete?name="
								+ URLEncoder.encode(channelName, "UTF-8") + "\">,[Delete]</a>";
					}

				} else {
					responseString = responseString + " (Not subscribed to the channel) <a href=\"/subscribe?name="
							+ URLEncoder.encode(channelName, "UTF-8") + "\">[Subscribe]</a>";
					if (isCreator) {
						responseString = responseString + "<a href=\"/delete?name="
								+ URLEncoder.encode(channelName, "UTF-8") + "\">,[Delete]</a>";
					}

				}
				responseString = responseString + "<br>XPath=\"<b>" + channel.getXPath() + "</b>\"</li>";
			}
			responseString = responseString + "</ul></div>";
			responseString = responseString + "<b>Create new channel:</b><br>"
					+ "<form action=\"/create\" method=\"GET\">Channel Name<br>"
					+ "<input type=\"text\" placeholder=\"Enter Channel Name\" name=\"name\" required><br>"
					+ "XPath<br><input type=\"text\" placeholder=\"Enter XPath\" name=\"xpath\" required><br>"
					+ "<button type=\"submit\">Create</button></form>" + "<a href=\"/logout\">Logout</a></body></html>";
			return responseString;
		});

		// If the name and password correspond to a register user start add the user
		// info in the session.
		// Either way redirect to the homepage
		post("/login", (request, response) -> {

			DBWrapper database = new DBWrapper(null);
			String name = request.queryParams("name");
			String pass = request.queryParams("password");
			pass = hash(pass);
			request.session().attribute("attempts", "true");
			if (name != null && pass != null) {
				UserValue user;
				UserKey userkey = new UserKey(name);

				user = database.getEntry(userkey);

				if (user != null && user.getPass().contentEquals(pass)) {
					request.session().attribute("username", name);
					request.session().attribute("firstName", user.getFirstName());
					request.session().attribute("lastName", user.getLastName());
				}
			}
			response.redirect("/");
			return "<html><body> Redirecting to <a href=\"/\">Home Page</a></body></html>";
		});

		// Look into the database for a page with the url specified by the query
		// parameters
		get("/lookup", (request, response) -> {
			try {
				String encodedURL = request.queryParams("url");
				if (encodedURL != null) {
					String decodedURL = URLDecoder.decode(encodedURL, "UTF-8");
					DBWrapper database = new DBWrapper(null);
					URLInfo url = new URLInfo(decodedURL, "*");
					PageValue page;
					PageKey pageKey = new PageKey(url.getHostName(), url.getFilePath(), url.getPortNo(),
							url.getProtocol());
					page = database.getEntry(pageKey);
					if (page == null || page.getContent() == null) {
						response.status(404);
						return "<html><body><b>404 Web Page not found</b><br><a href=\"/\">Main Page</a></body></html>";
					}
					response.type(page.getType());
					return page.getContent();

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		});

		// Show a form for an new user to register
		get("/newaccount", (request, response) -> {
			return "<html><body><b> Please fill the following form in order to create an account "
					+ "<form action=\"/register\" method=\"POST\">  <p><b>Username</b> <br>"
					+ "<input type=\"text\" placeholder=\"Enter Username\" name=\"name\" required> </p>"
					+ "<p><b>Password</b> <br> <input type=\"password\" placeholder=\"Enter Password\" name=\"password\" required> </p>"
					+ "<p><b>First Name</b> <br> <input type=\"text\" placeholder=\"Enter First Name\" name=\"firstName\" required> </p>"
					+ "<p><b>Last Name</b> <br> <input type=\"text\" placeholder=\"Enter Last Name\" name=\"lastName\" required> </p>"
					+ " <button type=\"submit\">Register</button>   </form><br><a href=\"/\">Main Page</a></body></html>";

		});

		// Check if the username and password is appropriate and store it in the
		// database,
		// otherwise show an error message
		post("/register", (request, response) -> {
			String name = request.queryParams("name");
			String pass = request.queryParams("password");
			String firstName = request.queryParams("firstName");
			String lastName = request.queryParams("lastName");
			String result = null;
			DBWrapper database = new DBWrapper(null);
			pass = hash(pass);
			if (name != null && pass != null) {
				UserValue user;
				UserKey userkey = new UserKey(name);

				user = database.getEntry(userkey);
				if (user == null) {
					UserValue uservalue = new UserValue(name, pass, firstName, lastName);
					database.addEntry(userkey, uservalue);
					request.session().attribute("username", name);
					request.session().attribute("firstName", firstName);
					request.session().attribute("lastName", lastName);
					response.redirect("/");
				} else {
					result = "<html><body>Sorry the username is already taken please go to <a href=\"/newaccount\">Account Registration</a> and try again<br><a href=\"/\">Main Page</a></body></html>";
				}
			} else {
				result = "<html><body>Sorry the username or the password are not formatted correctly, please go to <a href=\"/newaccount\">Account Registration</a> and try again<br><a href=\"/\">Main Page</a></body></html>";
			}
			return result;
		});

		// If a user is logged in create a new channel with the name and XPath given in
		// the query
		// parameters and make the user the creator of the channel.
		get("/create", (request, response) -> {
			UserChannelChecker checker = new XPathApp.UserChannelChecker(request, response);
			if (!checker.check)
				return checker.response_string;
			String username = checker.username;
			String channelName = checker.channelName;
			DBWrapper db = checker.db;
			UserValue user = checker.user;
			UserKey userKey = checker.userKey;
			String xpath = request.queryParams("xpath");
			if (xpath == null) {
				response.status(400);
				return "<html><body> Error 404. Bad request </body></html>";
			}
			String firstName = (String) (request.session().attribute("firstName"));
			String lastName = (String) (request.session().attribute("lastName"));
			/*
			 * if(username==null || firstName==null || lastName==null) {
			 * response.status(401); return
			 * "<html><body> Error 401. No user is logged in <br><a href=\"/\">Main Page</a></body></html>"
			 * ; } String channelName=request.queryParams("name"); String
			 * xpath=request.queryParams("xpath"); if(channelName==null || xpath==null) {
			 * response.status(400); return
			 * "<html><body> Error 404. Bad request </body></html>"; } DBWrapper db=new
			 * DBWrapper(null); ChannelValue channel; ChannelKey key=new
			 * ChannelKey(channelName); UserValue user; UserKey userkey=new
			 * UserKey(username); user=db.getEntry(userkey); if(user==null) {
			 * response.redirect("/logout"); return null; }
			 */
			ChannelValue channel;
			ChannelKey key = new ChannelKey(channelName);
			channel = db.getEntry(key);
			if (channel == null) {
				channel = new ChannelValue(channelName, xpath, username);
				db.addEntry(key, channel);
				user.addChannel(channelName);
				db.addEntry(userKey, user);
				return "<html><body> Hi " + firstName + ", you have succesfully created channel " + channelName
						+ " with<br> XPath=\"<b>" + xpath + "</b>\"<br><a href=\"/\">Main Page</a></body></html>";
			} else {
				response.status(409);
				return "<html><body> Error 409. " + channelName
						+ " already exists<br><a href=\"/\">Main Page</a></body></html>";
			}
		});

		// if a user is logged in delete the channel given from the name query parameter
		// only if the logged user is also the creator of the channel
		get("/delete", (request, response) -> {
			UserChannelChecker checker = new XPathApp.UserChannelChecker(request, response);
			if (!checker.check)
				return checker.response_string;
			String username = checker.username;
			String channelName = checker.channelName;
			DBWrapper db = checker.db;
			UserValue user = checker.user;
			UserKey userKey = checker.userKey;
			/*
			 * String username=(String)(request.session().attribute("username"));
			 * if(username==null) { response.status(401); return
			 * "<html><body> Error 401. No user is logged in <br><a href=\"/\">Main Page</a></body></html>"
			 * ; } String channelName=request.queryParams("name"); if(channelName==null) {
			 * response.status(400); return
			 * "<html><body> Erro 400. Bad Request. </body></html>"; } DBWrapper db=new
			 * DBWrapper(null); UserKey userKey=new UserKey(username); UserValue
			 * user=db.getEntry(userKey); if(user==null) { response.redirect("/logout");
			 * return null; }
			 */
			ChannelValue channel;
			ChannelKey key = new ChannelKey(channelName);
			channel = db.getEntry(key);
			if (channel == null) {
				response.status(404);
				return "<html><body> Error 404. Channel " + channelName
						+ " does not exist.<br><a href=\"/\">Main Page</a></body></html>";
			}
			if (channel.getCreator().contentEquals(username)) {
				db.deleteEntry(key);
				user.removeChannel(channelName);
				db.addEntry(userKey, user);
				return "<html><body> Channel " + channelName
						+ " succesfully deleted<br><a href=\"/\">Main Page</a></body></html>";
			} else {
				response.status(403);
				return "<html><body> Error 403. You are not the creator of  " + channelName
						+ " so you are not able to delete it<br><a href=\"/\">Main Page</a></body></html>";
			}
		});

		// if a user is logged in subscribe that user to the channel given from the name
		// query parameter.
		get("/subscribe", (request, response) -> {
			UserChannelChecker checker = new XPathApp.UserChannelChecker(request, response);
			if (!checker.check)
				return checker.response_string;
			String username = checker.username;
			String channelName = checker.channelName;
			DBWrapper db = checker.db;
			UserValue user = checker.user;
			UserKey userKey = checker.userKey;
			/*
			 * String username=(String)(request.session().attribute("username"));
			 * if(username==null) { response.status(401); return
			 * "<html><body> Error 401. No user is logged in <br><a href=\"/\">Main Page</a></body></html>"
			 * ; } String channelName=request.queryParams("name"); if(channelName==null) {
			 * response.status(400); return
			 * "<html><body> Erro 400. Bad Request. </body></html>"; } DBWrapper db=new
			 * DBWrapper(null); UserKey userKey=new UserKey(username); UserValue
			 * user=db.getEntry(userKey); if(user==null) { response.redirect("/logout");
			 * return null; }
			 */
			ChannelValue channel;
			ChannelKey key = new ChannelKey(channelName);
			channel = db.getEntry(key);
			if (channel == null) {
				response.status(404);
				return "<html><body> Error 404. Channel " + channelName
						+ " does not exist.<br><a href=\"/\">Main Page</a></body></html>";
			}
			user.addChannel(channelName);
			db.addEntry(userKey, user);
			return "<html><body> You succesfully subscribed to the channel " + channelName
					+ "<br><a href=\"/\">Main Page</a></body></html>";
		});

		// if a user is logged, unsubscribe the user from the channel given from the
		// name query parameter
		get("/unsubscribe", (request, response) -> {
			UserChannelChecker checker = new XPathApp.UserChannelChecker(request, response);
			if (!checker.check)
				return checker.response_string;
			String username = checker.username;
			String channelName = checker.channelName;
			DBWrapper db = checker.db;
			UserValue user = checker.user;
			UserKey userKey = checker.userKey;
			/*
			 * String username=(String)(request.session().attribute("username"));
			 * if(username==null) { response.status(401); return
			 * "<html><body> Error 401. No user is logged in <br><a href=\"/\">Main Page</a></body></html>"
			 * ; } String channelName=request.queryParams("name"); if(channelName==null) {
			 * response.status(400); return
			 * "<html><body> Erro 400. Bad Request. </body></html>"; } DBWrapper db=new
			 * DBWrapper(null); UserKey userKey=new UserKey(username); UserValue
			 * user=db.getEntry(userKey); if(user==null) { response.redirect("/logout");
			 * return null; }
			 */
			if (user.getChannelSet().contains(channelName)) {
				user.removeChannel(channelName);
				db.addEntry(userKey, user);
				return "<html><body> You succesfully unsubscribed from " + channelName
						+ "<br><a href=\"/\">Main Page</a></body></html>";
			} else {
				response.status(404);
				return "<html><body> Error 404. You are not subscribed to any channel with name " + channelName
						+ "<br><a href=\"/\">Main Page</a></body></html>";
			}
		});

		// if a user is logged in and subscribed to the channel given from the query
		// parameters, show the
		// crawled pages that match the channel's XPath
		get("/show", (request, response) -> {
			UserChannelChecker checker = new XPathApp.UserChannelChecker(request, response);
			if (!checker.check)
				return checker.response_string;
			String username = checker.username;
			String channelName = checker.channelName;
			DBWrapper db = checker.db;
			UserValue user = checker.user;
			/*
			 * String username=(String)(request.session().attribute("username"));
			 * if(username==null) { response.status(401); return
			 * "<html><body> Error 401. No user is logged in <br><a href=\"/\">Main Page</a></body></html>"
			 * ; } String channelName=request.queryParams("name"); if(channelName==null) {
			 * response.status(400); return
			 * "<html><body> Error 400. Bad Request. </body></html>"; } DBWrapper db=new
			 * DBWrapper(null); UserKey userKey=new UserKey(username); UserValue
			 * user=db.getEntry(userKey); if(user==null) { response.redirect("/logout");
			 * return null; }
			 */
			ChannelValue channel;
			ChannelKey key = new ChannelKey(channelName);
			channel = db.getEntry(key);
			if (channel == null) {
				response.status(404);
				return "<html><body> Error 404. Channel " + channelName
						+ " does not exist.<br><a href=\"/\">Main Page</a></body></html>";
			} else if (user.getChannelSet() != null && !user.getChannelSet().contains(channelName)) {
				response.status(404);
				return "<html><body> Error 404. Your are not subscribed to channel " + channelName
						+ "<br><a href=\"/\">Main Page</a>";
			}

			HashSet<String> documents = channel.getSet();
			URLInfo docURL;
			String showChannel = "<html><body><div class=\"channelheader\"><div>Channel name: " + channelName
					+ ", created by: " + channel.getCreator() + "</div><br>";
			DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd\'T\'hh:mm:ss");
			for (String doc : documents) {
				docURL = new URLInfo(doc, "*");
				PageKey pagekey = new PageKey(docURL.getHostName(), docURL.getFilePath(), docURL.getPortNo(),
						docURL.getProtocol());
				PageValue pagevalue = db.getEntry(pagekey);
				String crawledDate = dateFormat.format(pagevalue.getDate());
				showChannel = showChannel + "Crawled on: " + crawledDate + "<br>" + "Location: " + doc
						+ "<br><div class=\"document\">";
				if (pagevalue.getType().contains("xml")) {
					showChannel = showChannel + "<xmp>";
				}
				showChannel = showChannel + (new String(pagevalue.getContent(), "UTF-8"));
				if (pagevalue.getType().contains("xml")) {
					showChannel = showChannel + "</xmp>";
				}
				showChannel = showChannel + "<br></div>";

			}

			showChannel = showChannel + "</div>";
			response.type("text/html");
			return showChannel;
		});

		// Shutdown the server
		get("/shutdown", (request, response) -> {
			stop();
			return null;
		});

		// Remove the user info from the session
		get("/logout", (request, response) -> {
			request.session().removeAttribute("username");
			request.session().removeAttribute("firstName");
			request.session().removeAttribute("lastName");
			response.redirect("/");
			return null;
		});
	}
}