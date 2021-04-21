package main.java.com.capstone.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotBlank;

import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Entity
@Table(name = "covid_world_monthly")
@EntityListeners(AuditingEntityListener.class)
public class CountryMonthlyData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @NotBlank
    private String country;
    @NotBlank
    @Column(name="country_name",nullable = false)
    private String countryName;
    @NotBlank
    private int year;
    @NotBlank
    private int month;
    @NotBlank
    @Column(name="monthly_new_cases",nullable = false)
    private double monthlyNewCases;
    @NotBlank
    @Column(name="avg_new_cases",nullable = false)
    private double avgNewCases;
    @NotBlank
    @Column(name="avg_new_deaths",nullable = false)
    private double avgNewDeaths;
    @NotBlank
    @Column(name="monthly_new_deaths",nullable = false)
    private double monthlyNewDeaths;
    @NotBlank
    @Column(name="monthly_tests",nullable = false)
    private double monthlyTests;
    @NotBlank
    @Column(name="total_cases",nullable = false)
    private double totalCases;
    @NotBlank
    @Column(name="total_deaths",nullable = false)
    private double totalDeaths;
    @NotBlank
    private long population;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public double getMonthlyNewCases() {
        return monthlyNewCases;
    }

    public void setMonthlyNewCases(double monthlyNewCases) {
        this.monthlyNewCases = monthlyNewCases;
    }

    public double getAvgNewCases() {
        return avgNewCases;
    }

    public void setAvgNewCases(double avgNewCases) {
        this.avgNewCases = avgNewCases;
    }

    public double getAvgNewDeaths() {
        return avgNewDeaths;
    }

    public void setAvgNewDeaths(double avgNewDeaths) {
        this.avgNewDeaths = avgNewDeaths;
    }

    public double getMonthlyNewDeaths() {
        return monthlyNewDeaths;
    }

    public void setMonthlyNewDeaths(double monthlyNewDeaths) {
        this.monthlyNewDeaths = monthlyNewDeaths;
    }

    public double getMonthlyTests() {
        return monthlyTests;
    }

    public void setMonthlyTests(double monthlyTests) {
        this.monthlyTests = monthlyTests;
    }

    public double getTotalCases() {
        return totalCases;
    }

    public void setTotalCases(double totalCases) {
        this.totalCases = totalCases;
    }

    public double getTotalDeaths() {
        return totalDeaths;
    }

    public void setTotalDeaths(double totalDeaths) {
        this.totalDeaths = totalDeaths;
    }

    public long getPopulation() {
        return population;
    }

    public void setPopulation(long population) {
        this.population = population;
    }

    public String getCountryName() {
        return countryName;
    }

    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }
}
