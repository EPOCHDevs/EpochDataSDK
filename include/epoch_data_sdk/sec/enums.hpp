#pragma once

#include <epoch_core/enum_wrapper.h>


// GLAZE IS ADDED AUTOMATICALLY IN EpochCore.cmake

/**
 * @brief Most common SEC EDGAR form types
 *
 * Reference: https://sec-api.io/docs/query-api
 * Note: There are 150+ form types total, this enum covers the most frequently used ones.
 * For unlisted form types, use the string overload of query methods.
 */
CREATE_ENUM(FormType,
    // ===== Periodic Reports (U.S. Companies) =====
    TenK,        // 10-K: Annual report
    TenKA,       // 10-K/A: Amended annual report
    TenQ,        // 10-Q: Quarterly report
    TenQA,       // 10-Q/A: Amended quarterly report
    EightK,      // 8-K: Current report (material events)
    EightKA,     // 8-K/A: Amended current report

    // ===== Periodic Reports (Foreign Issuers) =====
    TwentyF,     // 20-F: Annual report for non-Canadian foreign issuers
    FortyF,      // 40-F: Annual report for Canadian issuers
    SixK,        // 6-K: Current report for foreign issuers

    // ===== Registration Statements =====
    S1,          // S-1: IPO registration
    S1A,         // S-1/A: Amended S-1
    S3,          // S-3: Streamlined registration for seasoned issuers
    S3A,         // S-3/A: Amended S-3
    S4,          // S-4: Securities issued in M&A transactions
    S8,          // S-8: Employee benefit plan securities

    // ===== Foreign Issuer Registration =====
    F1,          // F-1: Foreign private issuer IPO
    F3,          // F-3: Follow-on offerings for foreign issuers
    F4,          // F-4: Cross-border M&A
    F6,          // F-6: American Depositary Receipts

    // ===== Proxy and Information Statements =====
    DEF14A,      // Definitive proxy statement
    DEFA14A,     // Additional proxy materials
    DEFM14A,     // Merger proxy statement
    PRE14A,      // Preliminary proxy statement
    SC13D,       // Schedule 13D: 5%+ ownership disclosure
    SC13DA,      // Schedule 13D/A: Amended 13D
    SC13G,       // Schedule 13G: Passive 5%+ ownership
    SC13GA,      // Schedule 13G/A: Amended 13G

    // ===== Insider Trading Forms =====
    Form3,       // Initial beneficial ownership statement
    Form4,       // Stock transaction reports
    Form5,       // Annual insider trading summary
    Form144,     // Restricted securities sale notice

    // ===== Institutional Holdings =====
    Form13F,     // 13F-HR: Institutional holdings report

    // ===== Investment Company Forms =====
    N1A,         // N-1A: Mutual fund and ETF registration
    N2,          // N-2: Closed-end fund registration
    NCSR,        // N-CSR: Semi-annual/annual shareholder reports
    NCEN,        // N-CEN: Annual census report for investment companies
    NPORT,       // N-PORT: Monthly portfolio holdings
    NPORTP,      // N-PORT-P: Partial portfolio data
    NPX,         // N-PX: Proxy voting records

    // ===== Private Offerings =====
    FormD,       // Form D: Private placement offerings
    FormC,       // Form C: Crowdfunding offerings

    // ===== Regulation A (Mini-IPO) =====
    Form1A,      // 1-A: Regulation A offering statement
    Form1K,      // 1-K: Regulation A annual report
    Form1Z,      // 1-Z: Regulation A exit report

    // ===== Prospectuses =====
    FourTwoFourB2,  // 424B2: Prospectus supplement
    FourTwoFourB3,  // 424B3: Prospectus supplement
    FourTwoFourB4,  // 424B4: Final prospectus

    // ===== Investment Adviser =====
    FormADV,     // ADV: Investment adviser registration

    // ===== Special Purpose =====
    Effect,      // EFFECT: Effectiveness notification
    ABS15G,      // ABS-15G: Asset-backed securities exemption

    // ===== Other Common Forms =====
    ElevenK,     // 11-K: Employee stock purchase plan annual report
    TwentyFiveH, // 25: Notification filed by issuer to report change in name

    // Special value for custom/unlisted form types
    Other
);

/**
 * @brief SEC Form 4 Transaction Codes
 *
 * Reference: https://www.sec.gov/edgar/searchedgar/ownershipformcodes.html
 */
CREATE_ENUM(TransactionCode,
    // General Transaction Codes
    P,       // Open market or private purchase
    S,       // Open market or private sale
    V,       // Transaction voluntarily reported earlier than required

    // Rule 16b-3 Transaction Codes
    A,       // Grant, award or other acquisition pursuant to Rule 16b-3(d)
    D,       // Disposition to the issuer pursuant to Rule 16b-3(e)
    F,       // Payment of exercise price or tax liability
    I,       // Discretionary transaction pursuant to Rule 16b-3(f)
    M,       // Exercise or conversion of derivative security exempted pursuant to Rule 16b-3

    // Derivative Securities Codes
    C,       // Conversion of derivative security
    E,       // Expiration of short derivative position
    H,       // Expiration (or cancellation) of long derivative position with value received
    O,       // Exercise of out-of-the-money derivative security
    X,       // Exercise of in-the-money or at-the-money derivative security

    // Additional Transaction Codes
    J,       // Other transaction (requires description)
    K,       // Equity swap or instrument with similar characteristics
    G,       // Gift

    // Special value for unlisted codes
    Other
);

/**
 * @brief Form 13F Security Type - Shares (SH) or Principal Amount (PRN)
 */
CREATE_ENUM(SecurityType,
    SH,      // Shares
    PRN,     // Principal amount
    Other
);

/**
 * @brief Form 13F Investment Discretion
 */
CREATE_ENUM(InvestmentDiscretion,
    SOLE,    // Sole discretion
    SHARED,  // Shared discretion
    DFND,    // Defined
    Other
);

namespace data_sdk::sec {


/**
 * @brief Convert epoch_core::FormType enum to API string format
 *
 * @param form_type The enum value
 * @return std::string The API-compatible string (e.g., "10-K", "8-K/A", "DEF 14A")
 */
inline std::string formTypeToString(epoch_core::FormType form_type) {
    switch (form_type) {
        // Periodic Reports (U.S.)
        case epoch_core::FormType::TenK: return "10-K";
        case epoch_core::FormType::TenKA: return "10-K/A";
        case epoch_core::FormType::TenQ: return "10-Q";
        case epoch_core::FormType::TenQA: return "10-Q/A";
        case epoch_core::FormType::EightK: return "8-K";
        case epoch_core::FormType::EightKA: return "8-K/A";

        // Periodic Reports (Foreign)
        case epoch_core::FormType::TwentyF: return "20-F";
        case epoch_core::FormType::FortyF: return "40-F";
        case epoch_core::FormType::SixK: return "6-K";

        // Registration Statements
        case epoch_core::FormType::S1: return "S-1";
        case epoch_core::FormType::S1A: return "S-1/A";
        case epoch_core::FormType::S3: return "S-3";
        case epoch_core::FormType::S3A: return "S-3/A";
        case epoch_core::FormType::S4: return "S-4";
        case epoch_core::FormType::S8: return "S-8";

        // Foreign Registration
        case epoch_core::FormType::F1: return "F-1";
        case epoch_core::FormType::F3: return "F-3";
        case epoch_core::FormType::F4: return "F-4";
        case epoch_core::FormType::F6: return "F-6";

        // Proxy Statements
        case epoch_core::FormType::DEF14A: return "DEF 14A";
        case epoch_core::FormType::DEFA14A: return "DEFA14A";
        case epoch_core::FormType::DEFM14A: return "DEFM14A";
        case epoch_core::FormType::PRE14A: return "PRE 14A";
        case epoch_core::FormType::SC13D: return "SC 13D";
        case epoch_core::FormType::SC13DA: return "SC 13D/A";
        case epoch_core::FormType::SC13G: return "SC 13G";
        case epoch_core::FormType::SC13GA: return "SC 13G/A";

        // Insider Trading
        case epoch_core::FormType::Form3: return "3";
        case epoch_core::FormType::Form4: return "4";
        case epoch_core::FormType::Form5: return "5";
        case epoch_core::FormType::Form144: return "144";

        // Institutional Holdings
        case epoch_core::FormType::Form13F: return "13F-HR";

        // Investment Companies
        case epoch_core::FormType::N1A: return "N-1A";
        case epoch_core::FormType::N2: return "N-2";
        case epoch_core::FormType::NCSR: return "N-CSR";
        case epoch_core::FormType::NCEN: return "N-CEN";
        case epoch_core::FormType::NPORT: return "N-PORT";
        case epoch_core::FormType::NPORTP: return "N-PORT-P";
        case epoch_core::FormType::NPX: return "N-PX";

        // Private Offerings
        case epoch_core::FormType::FormD: return "D";
        case epoch_core::FormType::FormC: return "C";

        // Regulation A
        case epoch_core::FormType::Form1A: return "1-A";
        case epoch_core::FormType::Form1K: return "1-K";
        case epoch_core::FormType::Form1Z: return "1-Z";

        // Prospectuses
        case epoch_core::FormType::FourTwoFourB2: return "424B2";
        case epoch_core::FormType::FourTwoFourB3: return "424B3";
        case epoch_core::FormType::FourTwoFourB4: return "424B4";

        // Investment Adviser
        case epoch_core::FormType::FormADV: return "ADV";

        // Special Purpose
        case epoch_core::FormType::Effect: return "EFFECT";
        case epoch_core::FormType::ABS15G: return "ABS-15G";

        // Other
        case epoch_core::FormType::ElevenK: return "11-K";
        case epoch_core::FormType::TwentyFiveH: return "25";

        case epoch_core::FormType::Other:
        default:
            return "";
    }
}

/**
 * @brief Convert epoch_core::TransactionCode enum to API string format
 *
 * @param code The enum value
 * @return std::string The API-compatible string (e.g., "P", "S", "A")
 */
inline std::string transactionCodeToString(epoch_core::TransactionCode code) {
    switch (code) {
        case epoch_core::TransactionCode::P: return "P";
        case epoch_core::TransactionCode::S: return "S";
        case epoch_core::TransactionCode::V: return "V";
        case epoch_core::TransactionCode::A: return "A";
        case epoch_core::TransactionCode::D: return "D";
        case epoch_core::TransactionCode::F: return "F";
        case epoch_core::TransactionCode::I: return "I";
        case epoch_core::TransactionCode::M: return "M";
        case epoch_core::TransactionCode::C: return "C";
        case epoch_core::TransactionCode::E: return "E";
        case epoch_core::TransactionCode::H: return "H";
        case epoch_core::TransactionCode::O: return "O";
        case epoch_core::TransactionCode::X: return "X";
        case epoch_core::TransactionCode::J: return "J";
        case epoch_core::TransactionCode::K: return "K";
        case epoch_core::TransactionCode::G: return "G";
        case epoch_core::TransactionCode::Other:
        default:
            return "";
    }
}

/**
 * @brief Convert epoch_core::SecurityType enum to API string format
 *
 * @param type The enum value
 * @return std::string The API-compatible string (e.g., "SH", "PRN")
 */
inline std::string securityTypeToString(epoch_core::SecurityType type) {
    switch (type) {
        case epoch_core::SecurityType::SH: return "SH";
        case epoch_core::SecurityType::PRN: return "PRN";
        case epoch_core::SecurityType::Other:
        default:
            return "";
    }
}

/**
 * @brief Convert epoch_core::InvestmentDiscretion enum to API string format
 *
 * @param discretion The enum value
 * @return std::string The API-compatible string (e.g., "SOLE", "SHARED", "DFND")
 */
inline std::string investmentDiscretionToString(epoch_core::InvestmentDiscretion discretion) {
    switch (discretion) {
        case epoch_core::InvestmentDiscretion::SOLE: return "SOLE";
        case epoch_core::InvestmentDiscretion::SHARED: return "SHARED";
        case epoch_core::InvestmentDiscretion::DFND: return "DFND";
        case epoch_core::InvestmentDiscretion::Other:
        default:
            return "";
    }
}

} // namespace data_sdk::sec
