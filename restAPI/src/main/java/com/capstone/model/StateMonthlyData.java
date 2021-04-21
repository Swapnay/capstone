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
@Table(name = "covid_usa_monthly")
@EntityListeners(AuditingEntityListener.class)
public class StateMonthlyData {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @NotBlank
    private String state;
    @NotBlank
    private int year;
    @NotBlank
    private int month;
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
    @Column(name="total_cases",nullable = false)
    private double totalCases;
    @NotBlank
    @Column(name="total_deaths",nullable = false)
    private double totalDeaths;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
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
}
