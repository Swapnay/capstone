package main.java.com.capstone.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityListeners;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotBlank;

import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Entity
@Table(name = "stocks_monthly")
@EntityListeners(AuditingEntityListener.class)
public class StockLine {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @NotBlank
    private String symbol;

    @NotBlank
    private String category_name;

    @NotBlank
    private int year;
    @NotBlank
    private int month;

    @NotBlank
    private double monthly_return_rate;

    @NotBlank
    private double monthly_avg;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getCategory_name() {
        return category_name;
    }

    public void setCategory_name(String category_name) {
        this.category_name = category_name;
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

    public double getMonthly_return_rate() {
        return monthly_return_rate;
    }

    public void setMonthly_return_rate(double monthly_return_rate) {
        this.monthly_return_rate = monthly_return_rate;
    }

    public double getMonthly_avg() {
        return monthly_avg;
    }

    public void setMonthly_avg(double monthly_avg) {
        this.monthly_avg = monthly_avg;
    }
}
