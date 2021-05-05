package thunder.SurveyOnKafka;


import java.util.ArrayList;

public class Survey {

	private int id;

	private String fname;
	private String lname;
	private String address;
	private String city;
	private String state;
	private String zip;
	private String phone;
	private String email;
	private String date;
	private ArrayList<String> campusPref;
	private String referencedThru;
	private String recommendToOthers;

	public Survey() {
	}

	public Survey(int id, String fname, String lname, String address, String city, String state, String zip, String phone,
			String email, String date, ArrayList<String> campusPref, String referencedThru, String recommendToOthers) {
		this.id = id;
		this.fname = fname;
		this.lname = lname;
		this.address = address;
		this.city = city;
		this.state = state;
		this.zip = zip;
		this.phone = phone;
		this.email = email;
		this.date = date;
		this.campusPref = campusPref;
		this.referencedThru = referencedThru;
		this.recommendToOthers = recommendToOthers;
	}

	public void updateWithNewSurvey(Survey newSurvey) {
		this.fname = newSurvey.fname;
		this.lname = newSurvey.lname;
		this.address = newSurvey.address;
		this.city = newSurvey.city;
		this.state = newSurvey.state;
		this.zip = newSurvey.zip;
		this.phone = newSurvey.phone;
		this.email = newSurvey.email;
		this.date = newSurvey.date;
		this.campusPref = newSurvey.campusPref;
		this.referencedThru = newSurvey.referencedThru;
		this.recommendToOthers = newSurvey.recommendToOthers;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getFname() {
		return fname;
	}

	public void setFname(String fname) {
		this.fname = fname;
	}

	public String getLname() {
		return lname;
	}

	public void setLname(String lname) {
		this.lname = lname;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public ArrayList<String> getCampusPref() {
		return campusPref;
	}

	public void setCampusPref(ArrayList<String> campusPref) {
		this.campusPref = campusPref;
	}

	public String getReferencedThru() {
		return referencedThru;
	}

	public void setReferencedThru(String referencedThru) {
		this.referencedThru = referencedThru;
	}

	public String getRecommendToOthers() {
		return recommendToOthers;
	}

	public void setRecommendToOthers(String recommendToOthers) {
		this.recommendToOthers = recommendToOthers;
	}

	@Override
	public String toString() {
		return "survey{" + id + ", " + fname + ", " + lname + ", " + address + ", " + city + ", " + state + ", " + zip
				+ ", " + phone + ", " + date + ", " + "" + campusPref + ", " + referencedThru + ", " + recommendToOthers
				+ "}";
	}
}
