package org.mastik.query;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A tree structure containing predicates
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class PredicatesTree {
    private static final PredicatesTree EMPTY = createAnd(null, null);

    /**
     * Returns an empty predicates tree
     *
     * @return Instance of an empty predicates tree
     */
    public static PredicatesTree emptyTree() {
        return EMPTY;
    }

    enum Clause {
        And,
        Or
    }

    private Clause clause;
    private List<HasContainer> predicates;
    private List<PredicatesTree> children;

    public PredicatesTree(Clause clause, List<HasContainer> predicates, List<PredicatesTree> childContainers) {
        this.clause = clause;
        this.predicates = predicates;
        this.children = childContainers;
    }

    /**
     * Whether the clause of the predicates is 'and'
     */
    public boolean isAnd() {
        return this.clause == Clause.And;
    }

    /**
     * Whether the clause of the predicates is 'or'
     */
    public boolean isOr() {
        return this.clause == Clause.Or;
    }

    /**
     * Returns the predicates stored in this tree
     */
    public List<HasContainer> predicates() {
        return Collections.unmodifiableList(this.predicates);
    }

    /**
     * Returns child trees stored in this tree
     */
    public List<PredicatesTree> children() {
        return Collections.unmodifiableList(this.children);
    }

    /**
     * Whether this tree has predicates
     */
    public boolean hasPredicates() {
        return this.predicates != null && !this.predicates.isEmpty();
    }

    /**
     * Whether this tree has child trees
     */
    public boolean hasChildren() {
        return this.children != null && !this.children.isEmpty();
    }

    /**
     * Whether this tree has predicates or child trees
     */
    public boolean isEmpty() {
        return !hasChildren() && !hasPredicates();
    }

    @Override
    public String toString() {
        return String.format("PredicatesTree{predicates=%s, children=%s}", this.predicates, this.children);
    }

    /**
     * Create an 'and' predicates tree
     *
     * @param predicates      'Has' predicates
     * @param children Child predicate trees
     * @return New instance of a predicates tree with 'and' clause
     */
    public static PredicatesTree createAnd(List<HasContainer> predicates, List<PredicatesTree> children) {
        return new PredicatesTree(Clause.And, predicates, children);
    }

    /**
     * Create an 'or' predicates tree
     *
     * @param predicates      'Has' predicates
     * @param children Child predicate trees
     * @return New instance of a predicates tree with 'or' clause
     */
    public static PredicatesTree createOr(List<HasContainer> predicates, List<PredicatesTree> children) {
        return new PredicatesTree(Clause.Or, predicates, children);
    }

    /**
     * Creates an 'and' predicates tree from multiple 'has' predicate
     *
     * @param predicates 'Has' predicates to create 'and' from
     * @return New instance of 'and' predicates tree with multiple 'has' predicates
     */
    public static PredicatesTree createFromPredicates(HasContainer... predicates) {
        return createAnd(Lists.newArrayList(predicates), null);
    }

    /**
     * Creates a new 'and' predicates tree by and'ing existing predicates
     *
     * @param trees Predicates trees create 'and' from
     * @return New instance of 'and' predicates tree
     */
    public static PredicatesTree and(PredicatesTree... trees) {
        return and(Lists.newArrayList(trees));
    }

    /**
     * Creates a unified 'and' predicates tree from a collection of predicates trees
     * @param trees Collection of predicates trees
     * @return Unified 'and' predicates tree, created from the given trees
     */
    public static PredicatesTree and(List<PredicatesTree> trees) {
        List<PredicatesTree> nonEmptyTrees = trees.stream()
                .filter(container -> !container.isEmpty())
                .collect(Collectors.toList());

        if (nonEmptyTrees.isEmpty()) {
            return emptyTree();
        }

        if (nonEmptyTrees.size() == 1) {
            return nonEmptyTrees.get(0);
        }

        List<HasContainer> predicates = Lists.newArrayList();
        List<PredicatesTree> children = Lists.newArrayList();

        nonEmptyTrees.forEach(tree -> {
            if (tree.isAnd()) {
                if (tree.hasPredicates()) {
                    predicates.addAll(tree.predicates());
                }

                if (tree.hasChildren()) {
                    children.addAll(tree.children());
                }
            } else {
                children.add(tree); // if it is an 'or' tree
            }
        });

        return createAnd(predicates, children);
    }

    /**
     * Creates an 'or' predicates tree from multiple 'has' predicate
     *
     * @param predicates 'Has' predicates to create 'or' from
     * @return New instance of a 'or' predicates tree with multiple 'has' predicates
     */
    public static PredicatesTree or(HasContainer... predicates) {
        return createOr(Lists.newArrayList(predicates), null);
    }

    /**
     * Creates a new 'or' predicates tree by or'ing existing predicates
     *
     * @param trees Predicates trees create 'or' from
     * @return New instance of 'and' predicates tree
     */
    public static PredicatesTree or(PredicatesTree... trees) {
        return or(Lists.newArrayList(trees));
    }

    /**
     * Creates a unified 'or' predicates tree from a collection of predicates trees
     * @param trees Collection of predicates trees
     * @return Unified 'or' predicates tree, created from the given trees
     */
    public static PredicatesTree or(List<PredicatesTree> trees) {
        List<PredicatesTree> nonEmptyTrees = trees.stream()
                .filter(container -> !container.isEmpty())
                .collect(Collectors.toList());

        if (nonEmptyTrees.isEmpty()) {
            return emptyTree();
        }

        if (nonEmptyTrees.size() == 1) {
            return nonEmptyTrees.get(0);
        }

        return createOr(Collections.emptyList(), nonEmptyTrees);
    }
}
