package com.redis.fraud.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Evaluates rule conditions written in the small Python-like DSL the fixtures
 * use (design doc §8.1; e.g. {@code cfg:rules}). Supported grammar:
 *
 * <pre>
 *   or      := and ('or' and)*
 *   and     := not ('and' not)*
 *   not     := 'not' not | comparison
 *   comparison := primary (('=='|'!='|'<'|'<='|'>'|'>='|'in') primary)?
 *   primary := NUMBER | STRING | '[' (primary (',' primary)*)? ']' | IDENT | '(' or ')'
 * </pre>
 *
 * Identifiers resolve against the feature/context map. {@code in} tests
 * membership in a collection (numeric-aware), which is how set-membership rules
 * (blacklists, VIP, watchlist) and {@code local_hour in [0,1,2,3,4]} are
 * expressed. Compiled expressions are cached per condition string.
 */
@Component
public class ConditionEvaluator {

    @FunctionalInterface
    interface Expr {
        Object eval(Map<String, Object> ctx);
    }

    private final Map<String, Expr> cache = new ConcurrentHashMap<>();

    public boolean evaluate(String condition, Map<String, Object> ctx) {
        Object v = cache.computeIfAbsent(condition, this::compile).eval(ctx);
        if (v instanceof Boolean b) {
            return b;
        }
        throw new IllegalStateException("Condition is not boolean: '" + condition + "' -> " + v);
    }

    Expr compile(String condition) {
        Parser parser = new Parser(tokenize(condition));
        Expr expr = parser.parseOr();
        parser.expect(Type.EOF);
        return expr;
    }

    // ---- truthiness / comparison helpers ----

    private static boolean truthy(Object o) {
        if (o == null) {
            return false;
        }
        if (o instanceof Boolean b) {
            return b;
        }
        throw new IllegalStateException("Expected boolean, got: " + o);
    }

    private static Double asNumber(Object o) {
        return o instanceof Number n ? n.doubleValue() : null;
    }

    private static boolean eq(Object l, Object r) {
        Double ln = asNumber(l);
        Double rn = asNumber(r);
        if (ln != null && rn != null) {
            return ln.doubleValue() == rn.doubleValue();
        }
        return Objects.equals(l, r);
    }

    private static boolean contains(Object collection, Object x) {
        if (!(collection instanceof Collection<?> c)) {
            return false;
        }
        for (Object e : c) {
            if (eq(e, x)) {
                return true;
            }
        }
        return false;
    }

    // ---- tokenizer ----

    private enum Type { NUMBER, STRING, IDENT, EQ, NE, LT, LE, GT, GE, IN, AND, OR, NOT, LP, RP, LB, RB, COMMA, EOF }

    private record Tok(Type type, String text) {
    }

    private static List<Tok> tokenize(String s) {
        List<Tok> out = new ArrayList<>();
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '(') {
                out.add(new Tok(Type.LP, "(")); i++;
            } else if (c == ')') {
                out.add(new Tok(Type.RP, ")")); i++;
            } else if (c == '[') {
                out.add(new Tok(Type.LB, "[")); i++;
            } else if (c == ']') {
                out.add(new Tok(Type.RB, "]")); i++;
            } else if (c == ',') {
                out.add(new Tok(Type.COMMA, ",")); i++;
            } else if (c == '=' && i + 1 < n && s.charAt(i + 1) == '=') {
                out.add(new Tok(Type.EQ, "==")); i += 2;
            } else if (c == '!' && i + 1 < n && s.charAt(i + 1) == '=') {
                out.add(new Tok(Type.NE, "!=")); i += 2;
            } else if (c == '<') {
                if (i + 1 < n && s.charAt(i + 1) == '=') { out.add(new Tok(Type.LE, "<=")); i += 2; }
                else { out.add(new Tok(Type.LT, "<")); i++; }
            } else if (c == '>') {
                if (i + 1 < n && s.charAt(i + 1) == '=') { out.add(new Tok(Type.GE, ">=")); i += 2; }
                else { out.add(new Tok(Type.GT, ">")); i++; }
            } else if (c == '\'' || c == '"') {
                int j = i + 1;
                StringBuilder sb = new StringBuilder();
                while (j < n && s.charAt(j) != c) {
                    sb.append(s.charAt(j));
                    j++;
                }
                if (j >= n) {
                    throw new IllegalArgumentException("Unterminated string in: " + s);
                }
                out.add(new Tok(Type.STRING, sb.toString()));
                i = j + 1;
            } else if (Character.isDigit(c) || (c == '-' && i + 1 < n && Character.isDigit(s.charAt(i + 1)))) {
                int j = i + 1;
                while (j < n && (Character.isDigit(s.charAt(j)) || s.charAt(j) == '.')) {
                    j++;
                }
                out.add(new Tok(Type.NUMBER, s.substring(i, j)));
                i = j;
            } else if (Character.isLetter(c) || c == '_') {
                int j = i;
                while (j < n && (Character.isLetterOrDigit(s.charAt(j)) || s.charAt(j) == '_')) {
                    j++;
                }
                String word = s.substring(i, j);
                out.add(new Tok(switch (word) {
                    case "and" -> Type.AND;
                    case "or" -> Type.OR;
                    case "not" -> Type.NOT;
                    case "in" -> Type.IN;
                    default -> Type.IDENT;
                }, word));
                i = j;
            } else {
                throw new IllegalArgumentException("Unexpected character '" + c + "' in: " + s);
            }
        }
        out.add(new Tok(Type.EOF, ""));
        return out;
    }

    // ---- parser (recursive descent) ----

    private final class Parser {
        private final List<Tok> toks;
        private int pos;

        Parser(List<Tok> toks) {
            this.toks = toks;
        }

        private Tok peek() {
            return toks.get(pos);
        }

        private Tok next() {
            return toks.get(pos++);
        }

        private boolean match(Type t) {
            if (peek().type() == t) {
                pos++;
                return true;
            }
            return false;
        }

        void expect(Type t) {
            if (!match(t)) {
                throw new IllegalArgumentException("Expected " + t + " but found " + peek().type());
            }
        }

        Expr parseOr() {
            Expr e = parseAnd();
            while (match(Type.OR)) {
                Expr r = parseAnd();
                Expr l = e;
                e = ctx -> truthy(l.eval(ctx)) || truthy(r.eval(ctx));
            }
            return e;
        }

        private Expr parseAnd() {
            Expr e = parseNot();
            while (match(Type.AND)) {
                Expr r = parseNot();
                Expr l = e;
                e = ctx -> truthy(l.eval(ctx)) && truthy(r.eval(ctx));
            }
            return e;
        }

        private Expr parseNot() {
            if (match(Type.NOT)) {
                Expr inner = parseNot();
                return ctx -> !truthy(inner.eval(ctx));
            }
            return parseComparison();
        }

        private Expr parseComparison() {
            Expr left = parsePrimary();
            Type t = peek().type();
            if (t == Type.EQ || t == Type.NE || t == Type.LT || t == Type.LE
                    || t == Type.GT || t == Type.GE || t == Type.IN) {
                next();
                Expr right = parsePrimary();
                return comparison(t, left, right);
            }
            return left;
        }

        private Expr parsePrimary() {
            Tok t = peek();
            switch (t.type()) {
                case NUMBER -> {
                    next();
                    double d = Double.parseDouble(t.text());
                    return ctx -> d;
                }
                case STRING -> {
                    next();
                    String str = t.text();
                    return ctx -> str;
                }
                case IDENT -> {
                    next();
                    String name = t.text();
                    return ctx -> ctx.get(name);
                }
                case LP -> {
                    next();
                    Expr e = parseOr();
                    expect(Type.RP);
                    return e;
                }
                case LB -> {
                    next();
                    List<Expr> items = new ArrayList<>();
                    if (peek().type() != Type.RB) {
                        items.add(parsePrimary());
                        while (match(Type.COMMA)) {
                            items.add(parsePrimary());
                        }
                    }
                    expect(Type.RB);
                    return ctx -> {
                        List<Object> vals = new ArrayList<>(items.size());
                        for (Expr it : items) {
                            vals.add(it.eval(ctx));
                        }
                        return vals;
                    };
                }
                default -> throw new IllegalArgumentException("Unexpected token: " + t.type());
            }
        }

        private Expr comparison(Type op, Expr le, Expr re) {
            return ctx -> {
                Object l = le.eval(ctx);
                Object r = re.eval(ctx);
                return switch (op) {
                    case IN -> contains(r, l);
                    case EQ -> eq(l, r);
                    case NE -> !eq(l, r);
                    default -> {
                        Double a = asNumber(l);
                        Double b = asNumber(r);
                        if (a == null || b == null) {
                            yield false;
                        }
                        yield switch (op) {
                            case LT -> a < b;
                            case LE -> a <= b;
                            case GT -> a > b;
                            case GE -> a >= b;
                            default -> throw new IllegalStateException("unreachable");
                        };
                    }
                };
            };
        }
    }
}
